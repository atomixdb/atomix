use common::host_info::HostInfo;
use proto::warden::RangeServerRequest;
use std::sync::Arc;
use std::sync::Mutex;
use std::{collections::HashMap, pin::Pin};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::error;

struct StreamHandle {
    task: tokio::task::JoinHandle<()>,
    cancel: CancellationToken,
}

pub struct StreamMultiplexer {
    sender: mpsc::Sender<(HostInfo, RangeServerRequest)>,
    streams: Arc<Mutex<HashMap<HostInfo, StreamHandle>>>,
    runtime: tokio::runtime::Handle,
}

impl StreamMultiplexer {
    pub fn new(
        buffer: usize,
        runtime: tokio::runtime::Handle,
    ) -> (Self, mpsc::Receiver<(HostInfo, RangeServerRequest)>) {
        let (tx, rx) = mpsc::channel(buffer);
        (
            Self {
                sender: tx,
                streams: Arc::new(Mutex::new(HashMap::new())),
                runtime,
            },
            rx,
        )
    }

    pub fn add_stream(
        &self,
        key: HostInfo,
        mut stream: Pin<Box<dyn Stream<Item = Result<RangeServerRequest, Status>> + Send>>,
    ) {
        let cancel_token = CancellationToken::new();
        let child_token = cancel_token.child_token();
        let tx = self.sender.clone();
        let host_info = key.clone();

        let task = self.runtime.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = child_token.cancelled() => break,

                    msg = stream.next() => {
                        match msg {
                            Some(Ok(req)) => {
                                if tx.send((host_info.clone(), req)).await.is_err() {
                                    break;
                                }
                            }
                            Some(Err(e)) => {
                                error!("Stream error for host {:?}: {}", host_info, e);
                                break;
                            }
                            None => {
                                error!("Stream closed for host {:?}", host_info);
                                break;
                            }
                        }
                    }
                }
            }
        });

        let handle = StreamHandle {
            task,
            cancel: cancel_token,
        };

        self.streams.lock().unwrap().insert(key, handle);
    }

    pub fn remove_stream(&self, key: &HostInfo) {
        let handle = {
            let mut streams = self.streams.lock().unwrap();
            streams.remove(key)
        };
        if let Some(handle) = handle {
            self.runtime.spawn(async move {
                handle.cancel.cancel();
                let _ = handle.task.await; // Wait for clean shutdown
            });
        }
    }
}
