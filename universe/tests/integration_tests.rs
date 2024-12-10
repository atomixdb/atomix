use std::time::Duration;

use proto::universe::{
    get_keyspace_info_request::KeyspaceInfoSearchField, universe_client::UniverseClient,
    CreateKeyspaceRequest, GetKeyspaceInfoRequest, Keyspace, ListKeyspacesRequest,
};
use tokio::time::sleep;
use universe::{server::run_universe_server, storage::cassandra::Cassandra};
use uuid::Uuid;

#[tokio::test]
async fn test_create_and_list_keyspace_handlers() {
    let addr = "127.0.0.1:50056";
    // Create a Cassandra CQL session
    let storage = Cassandra::new("127.0.0.1:9042".to_string()).await;

    // Start the server in a separate task
    let server_task = tokio::spawn(async move {
        run_universe_server(addr, storage).await.unwrap();
    });

    // Wait a bit for the server to start
    sleep(Duration::from_millis(100)).await;

    // Create a Universe client
    let mut client = UniverseClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Create a keyspace
    let keyspace_name = format!("test_keyspace_{}", Uuid::new_v4());
    let namespace = "test_namespace";
    let primary_zone = Some(proto::universe::Zone {
        region: Some(proto::universe::Region {
            cloud: Some(proto::universe::region::Cloud::OtherCloud(
                "lalakis".to_string(),
            )),
            name: "test_region".to_string(),
        }),
        name: "test_zone".to_string(),
    });
    let base_key_ranges = vec![proto::universe::KeyRangeRequest {
        lower_bound_inclusive: vec![0],
        upper_bound_exclusive: vec![10],
    }];
    // No primary zone or base key ranges for now
    let keyspace_req = CreateKeyspaceRequest {
        name: keyspace_name.to_string(),
        namespace: namespace.to_string(),
        primary_zone,
        secondary_zones: vec![proto::universe::Zone {
            region: Some(proto::universe::Region {
                cloud: Some(proto::universe::region::Cloud::OtherCloud(
                    "lalakis".to_string(),
                )),
                name: "test_region_2".to_string(),
            }),
            name: "test_zone_2".to_string(),
        }],
        base_key_ranges,
    };
    let keyspace_id = client
        .create_keyspace(keyspace_req)
        .await
        .unwrap()
        .into_inner()
        .keyspace_id;

    // List keyspaces
    let keyspaces = client
        .list_keyspaces(ListKeyspacesRequest { region: None })
        .await
        .unwrap()
        .into_inner()
        .keyspaces;

    // Check that the keyspace we created is in the list
    assert!(keyspaces.iter().any(|k| k.keyspace_id == keyspace_id));

    // Get the keyspace info by keyspace_id
    let keyspace_info_request = GetKeyspaceInfoRequest {
        keyspace_info_search_field: Some(KeyspaceInfoSearchField::KeyspaceId(keyspace_id)),
    };

    let keyspace_info = client
        .get_keyspace_info(keyspace_info_request)
        .await
        .unwrap()
        .into_inner()
        .keyspace_info
        .unwrap();

    // Check that the keyspace info we got is the same as the one we created
    assert_eq!(keyspace_info.name, keyspace_name);
    assert_eq!(keyspace_info.namespace, namespace);

    //  Get the keyspace info by Keyspace
    let keyspace_info_request = GetKeyspaceInfoRequest {
        keyspace_info_search_field: Some(KeyspaceInfoSearchField::Keyspace(Keyspace {
            namespace: namespace.to_string(),
            name: keyspace_name.to_string(),
        })),
    };

    let keyspace_info = client
        .get_keyspace_info(keyspace_info_request)
        .await
        .unwrap()
        .into_inner()
        .keyspace_info
        .unwrap();

    // Check that the keyspace info we got is the same as the one we created
    assert_eq!(keyspace_info.name, keyspace_name);
    assert_eq!(keyspace_info.namespace, namespace);

    server_task.abort();
}
