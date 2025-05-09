use crate::network::for_testing::udp_fast_network::UdpFastNetwork;
use affinity;
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use tokio::runtime::Builder;
use tokio::sync::mpsc;

// Busy-polled network endpoint.
pub trait FastNetwork: Send + Sync + 'static {
    fn send(&self, to: SocketAddr, payload: Bytes) -> Result<(), std::io::Error>;
    fn listen_default(&self) -> mpsc::UnboundedReceiver<(SocketAddr, Bytes)>;
    // Listen for messages sent from a specific SocketAddr.
    fn register(&self, from: SocketAddr) -> mpsc::UnboundedReceiver<Bytes>;
    // Reads one message from the network (if any) and delivers it to all the
    // relevant listeners. Returns true if something was read from the network,
    // and false otherwise.
    fn poll(&self) -> bool;
}

pub async fn spawn_tokio_polling_thread(
    name: &str,
    fast_network: Arc<UdpFastNetwork>,
    core_id: usize,
) {
    thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            // Set CPU core affinity
            affinity::set_thread_affinity([core_id]).expect("Failed to set affinity");

            // Build a single-threaded Tokio runtime on this thread
            let rt = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to build Tokio runtime");

            // Run the polling loop inside the runtime
            rt.block_on(async move {
                loop {
                    fast_network.poll(); // sync/blocking call
                    tokio::task::yield_now().await; // yield to Tokio scheduler
                }
            });
        })
        .expect("Failed to spawn polling thread");
}
