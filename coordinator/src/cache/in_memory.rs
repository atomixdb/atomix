use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use common::keyspace_id::KeyspaceId;
use proto::universe::get_keyspace_info_request::KeyspaceInfoSearchField;
use proto::universe::universe_client::UniverseClient;
use proto::universe::{GetKeyspaceInfoRequest, Keyspace, KeyspaceInfo};
use std::collections::HashSet;
use std::hash::Hash;
use tokio::sync::mpsc;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tonic::async_trait;
use tracing::{error, warn};

use crate::cache::KeyspaceCache;
use crate::error::Error;

enum CacheEntry<T> {
    Value(T),
    Updating(broadcast::Sender<T>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CacheUpdateRequest {
    KeyspaceId(KeyspaceId),
    KeyspaceName(String, String),
}

pub struct InMemoryCache {
    name_to_id: Arc<RwLock<HashMap<(String, String), CacheEntry<KeyspaceId>>>>,
    id_to_info: Arc<RwLock<HashMap<KeyspaceId, CacheEntry<KeyspaceInfo>>>>,
    update_queue: IndexedSender<CacheUpdateRequest>,
    cache_updater_task: Option<JoinHandle<()>>,
    runtime: tokio::runtime::Handle,
}

impl InMemoryCache {
    pub fn new(
        universe_client: UniverseClient<tonic::transport::Channel>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let (update_queue, update_receiver) = indexed_channel(100);
        let name_to_id = Arc::new(RwLock::new(HashMap::new()));
        let id_to_info = Arc::new(RwLock::new(HashMap::new()));

        let name_to_id_clone = name_to_id.clone();
        let id_to_info_clone = id_to_info.clone();
        let retry_queue = update_queue.clone();
        let cache_updater_task = runtime.spawn(async move {
            Self::cache_updater(
                name_to_id_clone,
                id_to_info_clone,
                universe_client,
                retry_queue,
                update_receiver,
            )
            .await;
        });

        Self {
            name_to_id,
            id_to_info,
            update_queue,
            cache_updater_task: Some(cache_updater_task),
            runtime,
        }
    }

    async fn cache_updater(
        name_to_id: Arc<RwLock<HashMap<(String, String), CacheEntry<KeyspaceId>>>>,
        id_to_info: Arc<RwLock<HashMap<KeyspaceId, CacheEntry<KeyspaceInfo>>>>,
        mut universe_client: UniverseClient<tonic::transport::Channel>,
        retry_queue: IndexedSender<CacheUpdateRequest>,
        mut update_receiver: IndexedReceiver<CacheUpdateRequest>,
    ) {
        while let Some(request) = update_receiver.recv().await {
            let keyspace_info_search_field = match &request {
                CacheUpdateRequest::KeyspaceId(keyspace_id) => {
                    KeyspaceInfoSearchField::KeyspaceId(keyspace_id.id.to_string())
                }
                CacheUpdateRequest::KeyspaceName(name, namespace) => {
                    KeyspaceInfoSearchField::Keyspace(Keyspace {
                        name: name.clone(),
                        namespace: namespace.clone(),
                    })
                }
            };
            let keyspace_info = universe_client
                .get_keyspace_info(GetKeyspaceInfoRequest {
                    keyspace_info_search_field: Some(keyspace_info_search_field),
                })
                .await;
            let keyspace_info = match keyspace_info {
                Ok(keyspace_info) => {
                    let keyspace_info = keyspace_info.into_inner().keyspace_info.unwrap();
                    keyspace_info.clone()
                }
                Err(e) => {
                    error!("Failed to get keyspace info: {}", e);
                    // Retry in a bit
                    retry_queue.send(request).await.unwrap();
                    continue;
                }
            };
            let name = keyspace_info.name.clone();
            let namespace = keyspace_info.namespace.clone();
            let keyspace_id = KeyspaceId::from_str(&keyspace_info.keyspace_id).unwrap();

            // Lock both name_to_id and id_to_info
            let mut name_to_id = name_to_id.write().await;
            let mut id_to_info = id_to_info.write().await;

            // Update each cache entry. If the entry exists, notify the receivers.
            let name_entry = name_to_id.get(&(name.clone(), namespace.clone()));
            if let Some(entry) = name_entry {
                match entry {
                    CacheEntry::Updating(sender) => {
                        sender.send(keyspace_id.clone()).unwrap();
                    }
                    _ => {}
                }
            }
            name_to_id.insert((name, namespace), CacheEntry::Value(keyspace_id));

            let id_entry = id_to_info.get(&keyspace_id);
            if let Some(entry) = id_entry {
                match entry {
                    CacheEntry::Updating(sender) => {
                        sender.send(keyspace_info.clone()).unwrap();
                    }
                    _ => {}
                }
            }
            id_to_info.insert(keyspace_id, CacheEntry::Value(keyspace_info));
        }
    }
}

#[async_trait]
impl KeyspaceCache for InMemoryCache {
    async fn get_keyspace_id(&self, name: &str, namespace: &str) -> Result<KeyspaceId, Error> {
        let key = (name.to_string(), namespace.to_string());
        // Fast path: The keyspace id is already in the cache
        {
            let name_to_id = self.name_to_id.read().await;
            let keyspace_id = name_to_id.get(&key);
            if let Some(CacheEntry::Value(keyspace_id)) = keyspace_id {
                return Ok(keyspace_id.clone());
            }
        }

        // Slow path: The keyspace id is not in the cache, so we need to fetch it
        {
            let mut name_to_id = self.name_to_id.write().await;
            // Check again if the keyspace id is in the cache
            let cache_entry = name_to_id.get(&key);
            let mut rx = match cache_entry {
                Some(CacheEntry::Value(keyspace_id)) => {
                    return Ok(keyspace_id.clone());
                }
                // Someone else kicked off the cache update, just wait
                Some(CacheEntry::Updating(sender)) => sender.subscribe(),
                // Kick off the cache update
                None => {
                    self.update_queue
                        .send(CacheUpdateRequest::KeyspaceName(
                            name.to_string(),
                            namespace.to_string(),
                        ))
                        .await
                        .unwrap();
                    let (tx, _) = broadcast::channel(1);
                    let rx = tx.subscribe();
                    name_to_id.insert(key, CacheEntry::Updating(tx));
                    rx
                }
            };
            drop(name_to_id);
            rx.recv()
                .await
                .map_err(|e| Error::InternalError(Arc::new(e)))
        }
    }

    async fn get_keyspace_info(&self, keyspace_id: KeyspaceId) -> Result<KeyspaceInfo, Error> {
        // Fast path: The keyspace info is already in the cache
        {
            let id_to_info = self.id_to_info.read().await;
            let keyspace_info = id_to_info.get(&keyspace_id);
            if let Some(CacheEntry::Value(keyspace_info)) = keyspace_info {
                return Ok(keyspace_info.clone());
            }
        }

        // Slow path: The keyspace info is not in the cache, so we need to fetch it
        {
            let mut id_to_info = self.id_to_info.write().await;
            // Check again if the keyspace info is in the cache
            let cache_entry = id_to_info.get(&keyspace_id);
            let mut rx = match cache_entry {
                Some(CacheEntry::Value(keyspace_info)) => {
                    return Ok(keyspace_info.clone());
                }
                // Someone else kicked off the cache update, just wait
                Some(CacheEntry::Updating(sender)) => sender.subscribe(),
                // Kick off the cache update
                None => {
                    self.update_queue
                        .send(CacheUpdateRequest::KeyspaceId(keyspace_id))
                        .await
                        .unwrap();
                    let (tx, _) = broadcast::channel(1);
                    let rx = tx.subscribe();
                    id_to_info.insert(keyspace_id, CacheEntry::Updating(tx));
                    rx
                }
            };
            drop(id_to_info);
            rx.recv()
                .await
                .map_err(|e| Error::InternalError(Arc::new(e)))
        }
    }
}

pub fn indexed_channel<T>(capacity: usize) -> (IndexedSender<T>, IndexedReceiver<T>) {
    let (tx, rx) = mpsc::channel(capacity);
    (
        IndexedSender {
            queue: tx,
            set: Arc::new(RwLock::new(HashSet::new())),
        },
        IndexedReceiver {
            queue: rx,
            set: Arc::new(RwLock::new(HashSet::new())),
        },
    )
}

struct IndexedSender<T> {
    queue: mpsc::Sender<T>,
    set: Arc<RwLock<HashSet<T>>>,
}

impl<T> IndexedSender<T>
where
    T: Clone + Eq + Hash + Send + Sync + 'static,
{
    pub async fn send(&self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        let mut set = self.set.write().await;
        match set.contains(&value) {
            false => {
                set.insert(value.clone());
                self.queue.send(value).await
            }
            true => Ok(()),
        }
    }
}

impl<T> Clone for IndexedSender<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            set: Arc::clone(&self.set),
        }
    }
}

struct IndexedReceiver<T> {
    queue: mpsc::Receiver<T>,
    set: Arc<RwLock<HashSet<T>>>,
}

impl<T> IndexedReceiver<T>
where
    T: Clone + Eq + Hash + Send + Sync + 'static,
{
    pub async fn recv(&mut self) -> Option<T> {
        let value = self.queue.recv().await;
        match value {
            Some(value) => {
                self.set.write().await.remove(&value);
                Some(value)
            }
            None => None,
        }
    }
}
