use bytes::Bytes;
use common::config::{
    CassandraConfig, Config, EpochConfig, EpochPublisher, EpochPublisherSet, FrontendConfig,
    RangeServerConfig, RegionConfig, UniverseConfig,
};
use common::network::for_testing::udp_fast_network::UdpFastNetwork;
use common::region::{Region, Zone};
use std::time;
use uuid::Uuid;

use coordinator::keyspace::Keyspace;
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::net::UdpSocket;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use frontend::for_testing::{
    mock_epoch_publisher::MockEpochPublisher, mock_universe::MockUniverse,
};
use frontend::{frontend::Server, range_assignment_oracle::RangeAssignmentOracle};
use tracing::info;

use proto::frontend::frontend_client::FrontendClient;
use proto::frontend::{
    CommitRequest, GetRequest, Keyspace as ProtoKeyspace, PutRequest, StartTransactionRequest,
};
use proto::universe::{CreateKeyspaceRequest, KeyRangeRequest, Zone as ProtoZone};

static RUNTIME: Lazy<tokio::runtime::Runtime> =
    Lazy::new(|| tokio::runtime::Runtime::new().unwrap());

struct TestContext {
    keyspace: Keyspace,
    zone: ProtoZone,
    base_key_ranges: Vec<KeyRangeRequest>,
    client: FrontendClient<tonic::transport::Channel>,
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
        proto_server_addr: "127.0.0.1:50052".parse().unwrap(),
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

    //  Start the mock epoch publisher
    //  Start as many MockEpochPublishers as there are epoch publishers in the config
    for publisher in config.regions.get(&zone.region).unwrap().epoch_publishers.iter() {
        let fast_network_addr = publisher.publishers.iter().next().unwrap().fast_network_addr.clone();
        MockEpochPublisher::start(fast_network_addr.to_string(), CancellationToken::new()).await.unwrap();
    }
    //  Start the mock range server
    // MockRangeServer::start(&config).await;

    //  Start the Frontend server
    RUNTIME.spawn(async move {
        let fast_network_addr = &config.frontend.fast_network_addr;
        let fast_network = Arc::new(UdpFastNetwork::new(
            UdpSocket::bind(fast_network_addr).unwrap(),
        ));
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
        namespace: "test".to_string(),
        name: "bubbles".to_string(),
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
            primary_zone: Some(context.zone.into()),
            base_key_ranges: context.base_key_ranges,
        })
        .await
        .unwrap();
    let keyspace_id = response.get_ref().keyspace_id.clone();
    println!("Created keyspace with ID: {:?}", keyspace_id);

    // ----- Start transaction -----
    let response = context
        .client
        .start_transaction(StartTransactionRequest {})
        .await
        .unwrap();
    let transaction_id = Uuid::parse_str(&response.get_ref().transaction_id).unwrap();
    println!("Started transaction with ID: {:?}", transaction_id);

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
            value: Bytes::from_static(b"bubbles").to_vec(),
        })
        .await
        .unwrap();

    // ----- Get value from keyspace, key -----
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
    println!("Value: {:?}", value);

    // // ----- Commit transaction -----
    // context
    //     .client
    //     .commit(CommitRequest {
    //         transaction_id: transaction_id.to_string(),
    //     })
    //     .await
    //     .unwrap();
}
