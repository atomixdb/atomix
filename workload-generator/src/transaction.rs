use common::keyspace::Keyspace;
use frontend::error::Error as FrontendError;
use proto::frontend::frontend_client::FrontendClient;
use proto::frontend::Keyspace as ProtoKeyspace;
use proto::frontend::{CommitRequest, GetRequest, PutRequest, StartTransactionRequest};
use std::collections::HashMap;
use tracing::info;
use uuid::Uuid;

#[derive(Debug)]
pub struct Transaction {
    keyspace: Keyspace,
    readset: Vec<usize>,
    writeset: Option<Vec<usize>>,
}

impl Transaction {
    pub fn new(keyspace: Keyspace, readset: Vec<usize>, writeset: Option<Vec<usize>>) -> Self {
        Self {
            keyspace,
            readset,
            writeset,
        }
    }

    pub async fn execute(
        &self,
        client: &mut FrontendClient<tonic::transport::Channel>,
    ) -> Result<(), FrontendError> {
        let mut results = HashMap::new();

        // Start a transaction
        let response = client
            .start_transaction(StartTransactionRequest {})
            .await
            .unwrap();
        let transaction_id = Uuid::parse_str(&response.get_ref().transaction_id).unwrap();
        // let transaction_id_int = transaction_id.as_u128() as u64;
        // info!("Started transaction with ID: {:?}", transaction_id_int);

        for key in &self.readset {
            let response = client
                .get(GetRequest {
                    transaction_id: transaction_id.to_string(),
                    keyspace: Some(ProtoKeyspace {
                        namespace: self.keyspace.namespace.clone(),
                        name: self.keyspace.name.clone(),
                    }),
                    key: vec![*key as u8],
                })
                .await
                .unwrap();
            // let value_bytes = response.get_ref().value.as_ref();
            // let value_display = value_bytes
            //     .map(|bytes| {
            //         String::from_utf8(bytes.clone())
            //             .unwrap_or_else(|_| format!("<invalid utf8: {:?}>", bytes))
            //             .parse::<u64>()
            //             .unwrap_or(0)
            //     })
            //     .unwrap_or(0);
            // info!(
            //     "Read key: {:?}, value: {:?} tx id: {:?}",
            //     key, value_display, transaction_id_int
            // );
            let value_int = match response.get_ref().value.as_ref() {
                Some(bytes) => String::from_utf8(bytes.clone())
                    .unwrap()
                    .parse::<u64>()
                    .unwrap(),
                None => 0,
            };
            results.insert(key, value_int);
        }

        // Write the keys
        if let Some(writeset) = &self.writeset {
            // info!(
            //     "Writing keys: {:?} tx id: {:?}",
            //     writeset, transaction_id_int
            // );
            for key in writeset {
                let value = results.get(key).unwrap() + 1;
                let _response = client
                    .put(PutRequest {
                        transaction_id: transaction_id.to_string(),
                        keyspace: Some(ProtoKeyspace {
                            namespace: self.keyspace.namespace.clone(),
                            name: self.keyspace.name.clone(),
                        }),
                        key: vec![*key as u8],
                        value: value.to_string().as_bytes().to_vec(),
                    })
                    .await
                    .unwrap();
            }
        }

        // Commit the transaction
        client
            .commit(CommitRequest {
                transaction_id: transaction_id.to_string(),
            })
            .await
            .unwrap();
        // info!(
        //     "Committed transaction with keys: {:?} tx id: {:?}",
        //     self.writeset, transaction_id_int
        // );

        Ok(())
    }
}
