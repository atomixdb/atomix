use bytes::Bytes;
use common::config::{Config, EpochConfig, FrontendConfig, RangeServerConfig, UniverseConfig};
use common::keyspace::Keyspace;
use common::region::{Region, Zone};
use proto::frontend::frontend_client::FrontendClient;
use proto::frontend::{
    AbortRequest, CommitRequest, DeleteRequest, GetRequest, Keyspace as FrontendKeyspace,
    PutRequest, StartTransactionRequest,
};
use proto::universe::universe_client::UniverseClient;
use proto::universe::{
    get_keyspace_info_request::KeyspaceInfoSearchField, CreateKeyspaceRequest,
    GetKeyspaceInfoRequest, KeyRangeRequest, Keyspace as ProtoKeyspace, Region as ProtoRegion,
    Zone as ProtoZone,
};
use std::env;
use std::fs::read_to_string;
use std::net::UdpSocket;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();
}

#[tokio::test]
async fn simple_e2e() {
    init_tracing();
    let config_path = "../configs/config.json";
    let config: Config = serde_json::from_str(&read_to_string(config_path).unwrap()).unwrap();

    // Get universe client
    let universe_addr = config.universe.proto_server_addr.to_string();
    let mut universe_client = UniverseClient::connect(format!("http://{}", universe_addr))
        .await
        .unwrap();
    info!("Connected to Universe server at {}", universe_addr);

    // ----- Create keyspace -----
    let name = "test_zone".to_string();
    let namespace = "test_namespace".to_string();
    let region = "test-region".to_string();
    let zone = "test-region/a".to_string();
    let zone_proto = ProtoZone {
        region: Some(ProtoRegion {
            cloud: None,
            name: region.clone(),
        }),
        name: zone.clone(),
    };

    // let response = universe_client
    //     .create_keyspace(CreateKeyspaceRequest {
    //         namespace: namespace.clone(),
    //         name: name.clone(),
    //         primary_zone: Some(zone_proto.clone()),
    //         secondary_zones: vec![zone_proto.clone()],
    //         base_key_ranges: vec![],
    //     })
    //     .await
    //     .unwrap();
    // let keyspace_id = response.get_ref().keyspace_id.clone();
    // info!("Created keyspace with ID: {:?}", keyspace_id);
    // // Sleep a bit so ranges can be assigned
    // tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let keyspace_info_request = GetKeyspaceInfoRequest {
        keyspace_info_search_field: Some(KeyspaceInfoSearchField::Keyspace(ProtoKeyspace {
            namespace: namespace.clone(),
            name: name.clone(),
        })),
    };

    let keyspace_info = universe_client
        .get_keyspace_info(keyspace_info_request)
        .await
        .unwrap();
    let keyspace = keyspace_info.get_ref().keyspace_info.as_ref().unwrap();
    info!("Keyspace: {:?}", keyspace);

    // Get the frontend client
    let frontend_addr = config.frontend.proto_server_addr.to_string();
    let mut frontend_client = FrontendClient::connect(format!("http://{}", frontend_addr))
        .await
        .unwrap();
    info!("Connected to Frontend server at {}", frontend_addr);

    // ----- Start transaction -----
    let response = frontend_client
        .start_transaction(StartTransactionRequest {})
        .await
        .unwrap();
    let transaction_id = Uuid::parse_str(&response.get_ref().transaction_id).unwrap();
    info!("Started transaction with ID: {:?}", transaction_id);

    // ----- Put key-value pair into keyspace -----
    frontend_client
        .put(PutRequest {
            transaction_id: transaction_id.to_string(),
            keyspace: Some(FrontendKeyspace {
                namespace: keyspace.namespace.clone(),
                name: keyspace.name.clone(),
            }),
            key: Bytes::from_static(&[5]).to_vec(),
            value: Bytes::from_static(&[100]).to_vec(),
        })
        .await
        .unwrap();
    info!("Put key-value pair into keyspace");

    // ----- Get value from keyspace, key -----
    //  This tests the "read your writes" path
    let value = frontend_client
        .get(GetRequest {
            transaction_id: transaction_id.to_string(),
            keyspace: Some(FrontendKeyspace {
                namespace: keyspace.namespace.clone(),
                name: keyspace.name.clone(),
            }),
            key: Bytes::from_static(&[5]).to_vec(),
        })
        .await
        .unwrap();
    info!("Got Value: {:?}", value.get_ref().value);
    assert_eq!(
        value.get_ref().value,
        Some(Bytes::from_static(&[100]).to_vec())
    );

    // ----- Commit transaction -----
    frontend_client
        .commit(CommitRequest {
            transaction_id: transaction_id.to_string(),
        })
        .await
        .unwrap();
    info!("Committed transaction");
}
