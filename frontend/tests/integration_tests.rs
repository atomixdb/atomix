use bytes::Bytes;
use common::config::{
    CassandraConfig, Config, EpochConfig, EpochPublisher, EpochPublisherSet, FrontendConfig,
    RangeServerConfig, RegionConfig, UniverseConfig,
};
use common::keyspace::Keyspace;
use common::network::for_testing::udp_fast_network::UdpFastNetwork;
use common::region::{Region, Zone};
use std::time;
use uuid::Uuid;

use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::net::UdpSocket;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use frontend::for_testing::{
    mock_epoch_publisher::MockEpochPublisher, mock_rangeserver::MockRangeServer,
    mock_universe::MockUniverse,
};
use frontend::{frontend::Server, range_assignment_oracle::RangeAssignmentOracle};
use tracing::info;

use proto::frontend::frontend_client::FrontendClient;
use proto::frontend::{
    AbortRequest, CommitRequest, DeleteRequest, GetRequest, Keyspace as ProtoKeyspace, PutRequest,
    StartTransactionRequest,
};
use proto::universe::{CreateKeyspaceRequest, KeyRangeRequest, Zone as ProtoZone};

use common::network::fast_network::FastNetwork;

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

struct TestContext {
    keyspace: Keyspace,
    zone: ProtoZone,
    base_key_ranges: Vec<KeyRangeRequest>,
    client: FrontendClient<tonic::transport::Channel>,
    cancellation_token: CancellationToken,
}

fn make_zone() -> Zone {
    Zone {
        region: Region {
            cloud: None,
            name: "test-region".into(),
        },
        name: "a".into(),
    }
}

async fn init_config() -> Config {
    let epoch_config = EpochConfig {
        // Not used in these tests.
        proto_server_addr: "127.0.0.1:1".parse().unwrap(),
        epoch_duration: time::Duration::from_millis(10),
    };
    let mut config = Config {
        range_server: RangeServerConfig {
            range_maintenance_duration: time::Duration::from_secs(1),
            proto_server_addr: "127.0.0.1:50054".parse().unwrap(),
            fast_network_addr: "127.0.0.1:50055".parse().unwrap(),
        },
        universe: UniverseConfig {
            proto_server_addr: "127.0.0.1:50056".parse().unwrap(),
        },
        frontend: FrontendConfig {
            proto_server_addr: "127.0.0.1:50057".parse().unwrap(),
            fast_network_addr: "127.0.0.1:50058".parse().unwrap(),
            transaction_overall_timeout: time::Duration::from_secs(10),
        },
        cassandra: CassandraConfig {
            cql_addr: "127.0.0.1:9042".parse().unwrap(),
        },
        regions: std::collections::HashMap::new(),
        epoch: epoch_config,
    };
    let epoch_publishers = HashSet::from([EpochPublisher {
        name: "ep1".to_string(),
        // Not used in these tests.
        backend_addr: "127.0.0.1:50051".parse().unwrap(),
        fast_network_addr: "127.0.0.1:50052".parse().unwrap(),
    }]);
    let epoch_publishers_set = EpochPublisherSet {
        name: "ps1".to_string(),
        zone: make_zone(),
        publishers: epoch_publishers,
    };
    let region_config = RegionConfig {
        warden_address: "127.0.0.1:50053".parse().unwrap(),
        epoch_publishers: HashSet::from([epoch_publishers_set]),
    };
    config.regions.insert(make_zone().region, region_config);
    config
}

async fn setup() -> TestContext {
    let config = init_config().await;
    let zone = make_zone();
    let zone_clone = zone.clone();
    let frontend_addr = config.frontend.proto_server_addr.to_string().clone();
    let cancellation_token = CancellationToken::new();

    // ----- Start as many MockEpochPublishers as there are epoch publishers in the config -----
    for publisher in config
        .regions
        .get(&zone.region)
        .unwrap()
        .epoch_publishers
        .iter()
    {
        let fast_network_addr = publisher
            .publishers
            .iter()
            .next()
            .unwrap()
            .fast_network_addr
            .clone();

        let fast_network: Arc<UdpFastNetwork> = Arc::new(UdpFastNetwork::new(
            UdpSocket::bind(&fast_network_addr).unwrap(),
        ));
        MockEpochPublisher::start(fast_network, cancellation_token.clone())
            .await
            .unwrap();
    }

    // ----- Start the MockRangeServer -----
    let fast_network: Arc<UdpFastNetwork> = Arc::new(UdpFastNetwork::new(
        UdpSocket::bind(&config.range_server.fast_network_addr).unwrap(),
    ));
    MockRangeServer::start(
        &config,
        zone.clone(),
        fast_network,
        cancellation_token.clone(),
    )
    .await
    .unwrap();

    // ----- Start the Frontend server -----
    let fast_network = Arc::new(UdpFastNetwork::new(
        UdpSocket::bind(&config.frontend.fast_network_addr).unwrap(),
    ));
    let fast_network_clone = fast_network.clone();
    RUNTIME.spawn(async move {
        loop {
            fast_network_clone.poll();
            tokio::task::yield_now().await
        }
    });

    RUNTIME.spawn(async move {
        let cancellation_token = CancellationToken::new();
        let universe_client = MockUniverse::start(&config).await.unwrap();
        let range_assignment_oracle = Arc::new(RangeAssignmentOracle::new(universe_client));

        let server = Server::new(
            config,
            zone_clone,
            fast_network,
            range_assignment_oracle,
            RUNTIME.handle().clone(),
            RUNTIME.handle().clone(),
            cancellation_token.clone(),
        )
        .await;
        Server::start(server).await;
    });

    // ----- Connect to the Frontend server -----
    let client: FrontendClient<tonic::transport::Channel>;
    loop {
        let client_result = FrontendClient::connect(format!("http://{}", frontend_addr)).await;
        match client_result {
            Ok(client_ok) => {
                client = client_ok;
                break;
            }
            Err(e) => {
                info!("Failed to connect to Frontend server: {:?}", e);
                tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
            }
        }
    }
    let keyspace = Keyspace {
        namespace: "test_namespace".to_string(),
        name: "test_name".to_string(),
    };
    let key_range_requests = vec![KeyRangeRequest {
        lower_bound_inclusive: vec![0],
        upper_bound_exclusive: vec![10],
    }];

    TestContext {
        keyspace,
        zone: ProtoZone::from(zone),
        base_key_ranges: key_range_requests,
        client,
        cancellation_token,
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

#[tokio::test]
async fn test_frontend() {
    let mut context = setup().await;

    // ----- Create keyspace -----
    let response = context
        .client
        .create_keyspace(CreateKeyspaceRequest {
            namespace: context.keyspace.namespace.clone(),
            name: context.keyspace.name.clone(),
            primary_zone: Some(context.zone.clone()),
            secondary_zones: vec![],
            base_key_ranges: context.base_key_ranges.clone(),
        })
        .await
        .unwrap();
    let keyspace_id = response.get_ref().keyspace_id.clone();
    info!("Created keyspace with ID: {:?}", keyspace_id);

    // ----- Start transaction -----
    let response = context
        .client
        .start_transaction(StartTransactionRequest {})
        .await
        .unwrap();
    let transaction_id = Uuid::parse_str(&response.get_ref().transaction_id).unwrap();
    info!("Started transaction with ID: {:?}", transaction_id);

    // ----- Put key-value pair into keyspace -----
    context
        .client
        .put(PutRequest {
            transaction_id: transaction_id.to_string(),
            keyspace: Some(ProtoKeyspace {
                namespace: context.keyspace.namespace.clone(),
                name: context.keyspace.name.clone(),
            }),
            key: Bytes::from_static(&[5]).to_vec(),
            value: Bytes::from_static(&[100]).to_vec(),
        })
        .await
        .unwrap();
    info!("Put key-value pair into keyspace");

    // ----- Get value from keyspace, key -----
    //  This tests the "read your writes" path
    let value = context
        .client
        .get(GetRequest {
            transaction_id: transaction_id.to_string(),
            keyspace: Some(ProtoKeyspace {
                namespace: context.keyspace.namespace.clone(),
                name: context.keyspace.name.clone(),
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
    context
        .client
        .commit(CommitRequest {
            transaction_id: transaction_id.to_string(),
        })
        .await
        .unwrap();
    info!("Committed transaction");

    // ----- Start new transaction -----
    let response = context
        .client
        .start_transaction(StartTransactionRequest {})
        .await
        .unwrap();
    let transaction_id = Uuid::parse_str(&response.get_ref().transaction_id).unwrap();
    info!("Started transaction with ID: {:?}", transaction_id);

    // ----- Get value from keyspace, key -----
    //  Gets value from the previous transaction
    let value = context
        .client
        .get(GetRequest {
            transaction_id: transaction_id.to_string(),
            keyspace: Some(ProtoKeyspace {
                namespace: context.keyspace.namespace.clone(),
                name: context.keyspace.name.clone(),
            }),
            key: Bytes::from_static(&[5]).to_vec(),
        })
        .await
        .unwrap();
    info!("Value: {:?}", value);
    assert_eq!(
        value.get_ref().value,
        Some(Bytes::from_static(&[100]).to_vec())
    );

    // ----- Delete key-value pair from keyspace -----
    context
        .client
        .delete(DeleteRequest {
            transaction_id: transaction_id.to_string(),
            keyspace: Some(ProtoKeyspace {
                namespace: context.keyspace.namespace.clone(),
                name: context.keyspace.name.clone(),
            }),
            key: Bytes::from_static(&[5]).to_vec(),
        })
        .await
        .unwrap();
    info!("Deleted key-value pair from keyspace");
    // ----- Put a new key-value pair into keyspace -----
    context
        .client
        .put(PutRequest {
            transaction_id: transaction_id.to_string(),
            keyspace: Some(ProtoKeyspace {
                namespace: context.keyspace.namespace.clone(),
                name: context.keyspace.name.clone(),
            }),
            key: Bytes::from_static(&[4]).to_vec(),
            value: Bytes::from_static(b"bubbles").to_vec(),
        })
        .await
        .unwrap();
    info!("Put a new key-value pair into keyspace");

    // ----- Abort the transaction -----
    context
        .client
        .abort(AbortRequest {
            transaction_id: transaction_id.to_string(),
        })
        .await
        .unwrap();
    info!("Aborted transaction");
}
