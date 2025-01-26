use epoch::storage::cassandra::Cassandra;
use epoch::storage::in_memory::InMemoryEpochStorage;
use epoch::storage::Error;
use epoch::storage::Storage;
use scylla::query::Query;
use scylla::statement::SerialConsistency;
use scylla::SessionBuilder;
use test_case::test_case;
use uuid::Uuid;

const CASSANDRA_KNOWN_NODE: &str = "127.0.0.1:9042";

// We need to wrap the implementation in an enum like this since the Storage trait is not "object
// safe" and therefore does not support dynamic dispatch.
enum StorageTestCase {
    InMemory(Option<InMemoryEpochStorage>),
    Cassandra {
        cass: Option<Cassandra>,
        region: String,
    },
}

impl StorageTestCase {
    fn new_in_memory() -> StorageTestCase {
        StorageTestCase::InMemory(None)
    }

    fn new_cassandra() -> StorageTestCase {
        StorageTestCase::Cassandra {
            cass: None,
            region: String::new(),
        }
    }

    async fn setup(&mut self) {
        match self {
            StorageTestCase::InMemory(storage) => {
                *storage = Some(InMemoryEpochStorage::new());
            }
            StorageTestCase::Cassandra { cass, region } => {
                *region = Uuid::new_v4().to_string();
                *cass =
                    Some(Cassandra::new(CASSANDRA_KNOWN_NODE.to_string(), region.clone()).await);
            }
        }
    }

    async fn teardown(self) {
        match self {
            StorageTestCase::InMemory(_) => {}
            StorageTestCase::Cassandra { cass: _, region } => {
                let session = SessionBuilder::new()
                    .known_node(CASSANDRA_KNOWN_NODE)
                    .build()
                    .await
                    .unwrap();
                let mut query = Query::new("DELETE FROM atomix.epoch WHERE region = ?");
                query.set_serial_consistency(Some(SerialConsistency::Serial));
                let _ = session.query(query, (region.clone(),)).await;
            }
        }
    }
}

impl Storage for StorageTestCase {
    async fn initialize_epoch(&self) -> Result<(), Error> {
        match self {
            StorageTestCase::InMemory(storage) => {
                storage.as_ref().unwrap().initialize_epoch().await
            }
            StorageTestCase::Cassandra { cass, .. } => {
                cass.as_ref().unwrap().initialize_epoch().await
            }
        }
    }

    async fn read_latest(&self) -> Result<u64, Error> {
        match self {
            StorageTestCase::InMemory(storage) => storage.as_ref().unwrap().read_latest().await,
            StorageTestCase::Cassandra { cass, .. } => cass.as_ref().unwrap().read_latest().await,
        }
    }

    async fn conditional_update(&self, new_epoch: u64, current_epoch: u64) -> Result<(), Error> {
        match self {
            StorageTestCase::InMemory(storage) => {
                storage
                    .as_ref()
                    .unwrap()
                    .conditional_update(new_epoch, current_epoch)
                    .await
            }
            StorageTestCase::Cassandra { cass, .. } => {
                cass.as_ref()
                    .unwrap()
                    .conditional_update(new_epoch, current_epoch)
                    .await
            }
        }
    }
}

#[tokio::test]
#[test_case(StorageTestCase::new_cassandra() ; "Cassandra")]
#[test_case(StorageTestCase::new_in_memory(); "InMemoryEpochStorage")]
async fn initialization_is_idempotent(mut storage: StorageTestCase) {
    storage.setup().await;

    storage.initialize_epoch().await.unwrap();
    assert_eq!(storage.read_latest().await.unwrap(), 1);

    storage.conditional_update(2, 1).await.unwrap();
    assert_eq!(storage.read_latest().await.unwrap(), 2);

    // Initialize again. This shouldn't return an error or affect the current epoch.
    storage.initialize_epoch().await.unwrap();
    assert_eq!(storage.read_latest().await.unwrap(), 2);

    storage.teardown().await;
}

#[tokio::test]
#[test_case(StorageTestCase::new_cassandra() ; "Cassandra")]
#[test_case(StorageTestCase::new_in_memory(); "InMemoryEpochStorage")]
async fn initialize_read_update(mut storage: StorageTestCase) {
    storage.setup().await;

    storage.initialize_epoch().await.unwrap();
    let epoch = storage.read_latest().await.unwrap();
    assert_eq!(epoch, 1);
    storage
        .conditional_update(/*new_epoch=*/ 3, /*current_epoch=*/ 1)
        .await
        .unwrap();
    let epoch = storage.read_latest().await.unwrap();
    assert_eq!(epoch, 3);

    storage.teardown().await;
}

#[tokio::test]
#[test_case(StorageTestCase::new_cassandra() ; "Cassandra")]
#[test_case(StorageTestCase::new_in_memory(); "InMemoryEpochStorage")]
async fn initialize_condition_failed(mut storage: StorageTestCase) {
    storage.setup().await;

    storage.initialize_epoch().await.unwrap();
    let epoch = storage.read_latest().await.unwrap();
    assert_eq!(epoch, 1);
    let err = storage
        .conditional_update(/*new_epoch=*/ 3, /*current_epoch=*/ 2)
        .await
        .expect_err("conditional update should fail if current epoch does not match");
    match err {
        Error::ConditionFailed => (),
        _ => panic!("unexpected error: {:?}", err),
    }

    storage.teardown().await;
}

#[tokio::test]
#[test_case(StorageTestCase::new_cassandra() ; "Cassandra")]
#[test_case(StorageTestCase::new_in_memory(); "InMemoryEpochStorage")]
async fn epoch_increases_monotonically(mut storage: StorageTestCase) {
    storage.setup().await;

    storage.initialize_epoch().await.unwrap();
    assert_eq!(storage.read_latest().await.unwrap(), 1);
    let err = storage
        .conditional_update(/*new_epoch=*/ 0, /*current_epoch=*/ 1)
        .await
        .expect_err("conditional update should not decrease the epoch");
    match err {
        Error::ConditionFailed => (),
        _ => panic!("unexpected error: {:?}", err),
    }
    // The epoch shouldn't have changed.
    assert_eq!(storage.read_latest().await.unwrap(), 1);

    storage.teardown().await;
}

// Tests conditional updates where the current epoch used is lower than the actual current epoch.
#[tokio::test]
#[test_case(StorageTestCase::new_cassandra() ; "Cassandra")]
#[test_case(StorageTestCase::new_in_memory(); "InMemoryEpochStorage")]
async fn low_epoch_conditional_update(mut storage: StorageTestCase) {
    storage.setup().await;

    // Initialize and advance the epoch to 2.
    storage.initialize_epoch().await.unwrap();
    storage
        .conditional_update(/*new_epoch=*/ 2, /*current_epoch=*/ 1)
        .await
        .expect("conditional update should succeed");
    assert_eq!(storage.read_latest().await.unwrap(), 2);

    // Attempt an update where the epoch is behind by one.
    match storage
        .conditional_update(/*new_epoch=*/ 2, /*current_epoch=*/ 1)
        .await
    {
        Err(Error::ConditionFailed) => (),
        Ok(_) => panic!("stale current epoch should not succeed"),
        _ => panic!("unexpected error"),
    }
    // The epoch shouldn't have changed.
    assert_eq!(storage.read_latest().await.unwrap(), 2);

    storage.teardown().await;
}

// Tests conditional updates where the current epoch used is higher than the actual current epoch.
#[tokio::test]
#[test_case(StorageTestCase::new_cassandra() ; "Cassandra")]
#[test_case(StorageTestCase::new_in_memory(); "InMemoryEpochStorage")]
async fn high_epoch_conditional_update(mut storage: StorageTestCase) {
    storage.setup().await;

    storage.initialize_epoch().await.unwrap();
    assert_eq!(storage.read_latest().await.unwrap(), 1);

    // Attempt an update where the epoch is ahead by one.
    match storage
        .conditional_update(/*new_epoch=*/ 3, /*current_epoch=*/ 2)
        .await
    {
        Err(Error::ConditionFailed) => (),
        Ok(_) => panic!("high current epoch should not succeed"),
        _ => panic!("unexpected error"),
    }
    // The epoch shouldn't have changed.
    assert_eq!(storage.read_latest().await.unwrap(), 1);

    storage.teardown().await;
}
