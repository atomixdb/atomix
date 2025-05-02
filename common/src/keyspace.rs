#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct Keyspace {
    pub namespace: String,
    pub name: String,
}
