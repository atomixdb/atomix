use std::ops::Deref;
use std::str::FromStr;

use bytes::Bytes;
use common::{full_range_id::FullRangeId, keyspace_id::KeyspaceId};
use proto::frontend::frontend_client::FrontendClient;
use proto::frontend::{PutRequest, StartTransactionRequest};
use proto::universe::{
    get_keyspace_info_request::KeyspaceInfoSearchField, CreateKeyspaceRequest,
    GetKeyspaceInfoRequest, KeyRangeRequest, Keyspace as ProtoKeyspace, ListKeyspacesRequest,
    ListKeyspacesResponse, Region as ProtoRegion, Zone as ProtoZone,
};
use proto::{
    frontend::{CommitRequest, GetRequest, Keyspace as FrontendKeyspace},
    universe::universe_client::UniverseClient,
};
use tracing::info;
use uuid::Uuid;

use crate::helpers::{create_keyspace_if_not_exists, init_tracing, SimpleCluster};

async fn do_one_tx(
    frontend_client: &mut FrontendClient<tonic::transport::Channel>,
    keyspace: &proto::universe::KeyspaceInfo,
) -> uuid::Uuid {
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
    let _ = frontend_client
        .commit(CommitRequest {
            transaction_id: transaction_id.to_string(),
        })
        .await
        .unwrap();
    info!("Committed transaction");

    transaction_id
}

#[tokio::test]
async fn test_replication() {
    init_tracing();
    let cluster = SimpleCluster::new().await;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let config = cluster.config.clone();

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

    let keyspace =
        create_keyspace_if_not_exists(&mut universe_client, namespace, name, zone_proto).await;
    info!("Keyspace: {:?}", keyspace);

    // Get the frontend client
    let frontend_addr = config.frontend.proto_server_addr.to_string();
    let mut frontend_client = FrontendClient::connect(format!("http://{}", frontend_addr))
        .await
        .unwrap();
    info!("Connected to Frontend server at {}", frontend_addr);

    // ----- Start two transactions -----
    let transaction_id_1 = do_one_tx(&mut frontend_client, &keyspace).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let transaction_id_2 = do_one_tx(&mut frontend_client, &keyspace).await;

    // ----- Check that the write was replicated -----

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    // Get the secondary range
    let secondary_range = keyspace.secondary_key_ranges.first().unwrap();
    let secondary_range_id = secondary_range
        .key_range
        .as_ref()
        .unwrap()
        .base_range_uuid
        .clone();
    let secondary_range_id = Uuid::parse_str(&secondary_range_id).unwrap();

    let secondary_loaded_ranges_guard = cluster
        .range_server_handle
        .server
        .secondary_loaded_ranges
        .read()
        .await;
    let secondary_range_manager = secondary_loaded_ranges_guard
        .get(&secondary_range_id)
        .unwrap_or_else(|| panic!("Secondary range not found: {:?}", secondary_range_id))
        .clone();
    drop(secondary_loaded_ranges_guard);
    let secondary_range_id = FullRangeId {
        keyspace_id: KeyspaceId::from_str(&keyspace.keyspace_id).unwrap(),
        range_id: secondary_range_id.clone(),
    };
    let state = secondary_range_manager.state.read().await;
    let wal_epoch = match state.deref() {
        rangeserver::secondary_range_manager::r#impl::State::Loaded(s) => {
            // Check that wal epoch is set to the commit epoch
            let wal_epoch = s.wal_epoch.get();
            wal_epoch.unwrap_or_else(|| {
                panic!(
                    "rangeserver:Wal epoch not found for range {:?}",
                    secondary_range_id
                )
            })
        }
        _ => panic!("Secondary range not loaded"),
    };
    info!(
        "Rangeserver has loaded the secondary range and the wal epoch is {}",
        wal_epoch
    );

    // ----- Check that the warden knows the status -----
    {
        let range_statuses = cluster
            .warden_handle
            .assignment_computation
            .range_statuses
            .read()
            .unwrap();
        let range_status = range_statuses
            .get(&secondary_range_id)
            .unwrap_or_else(|| panic!("warden: Range status not found: {:?}", secondary_range_id));
        let actual_applied_epoch = match range_status {
            warden::assignment_computation::RangeStatus::Loaded(
                warden::assignment_computation::LoadedRangeStatus::Secondary(status),
            ) => {
                let wal_epoch_at_warden = status.wal_epoch.unwrap_or_else(|| {
                    panic!(
                        "warden: Wal epoch not found for range {:?}",
                        secondary_range_id
                    )
                });
                assert_eq!(wal_epoch_at_warden, wal_epoch);
                status.applied_epoch
            }
            _ => panic!("Range status not loaded"),
        };
        info!("Successfully checked that the warden knows the status and wal epoch");

        // ----- Check that the wal epoch is set to the commit epoch -----
        let keyspace_id = KeyspaceId::from_str(&keyspace.keyspace_id).unwrap();
        let desired_applied_epochs = cluster
            .warden_handle
            .assignment_computation
            .desired_applied_epochs
            .lock()
            .unwrap();
        let desired_applied_epoch = desired_applied_epochs.get(&keyspace_id).unwrap_or_else(|| {
            panic!(
                "warden: Desired applied epoch not found for keyspace {:?}",
                keyspace_id
            )
        });
        assert_eq!(*desired_applied_epoch, wal_epoch);
        info!("Successfully checked that the desired applied epoch is set to the wal epoch");

        // ----- Print the actual applied epoch -----
        info!(
            "Actual applied epoch: {}",
            actual_applied_epoch.unwrap_or_else(|| {
                panic!(
                    "warden: Applied epoch not found for range {:?}",
                    secondary_range_id
                )
            })
        );
    }

    // ----- Shutdown -----
    cluster.shutdown().await;
}
