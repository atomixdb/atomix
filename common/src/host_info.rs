use std::net::SocketAddr;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct HostIdentity {
    pub name: String,
    pub zone: crate::region::Zone,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HostInfo {
    pub identity: HostIdentity,
    pub address: SocketAddr,
    pub warden_connection_epoch: u64,
}
