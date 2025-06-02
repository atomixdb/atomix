use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct WorkloadConfig {
    #[serde(rename = "num-queries")]
    pub num_queries: u64,
    #[serde(rename = "num-keys")]
    pub num_keys: u64,
    #[serde(rename = "zipf-exponent")]
    pub zipf_exponent: f64,
    pub namespace: String,
    pub name: String,
    #[serde(rename = "max-concurrency")]
    pub max_concurrency: u64,
    #[serde(rename = "background-runtime-core-ids")]
    pub background_runtime_core_ids: Vec<u32>,
}
