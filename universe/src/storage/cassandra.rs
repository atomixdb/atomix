use std::str::FromStr;

use super::*;
use proto::universe::{KeyRange, KeyspaceInfo, Zone, ZonedKeyRange};
use scylla::macros::{FromUserType, SerializeValue};
use scylla::query::Query;
use scylla::statement::SerialConsistency;
use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;
use scylla::transport::PagingState;
use scylla::Session;
use scylla::{FromRow, SerializeRow, SessionBuilder};
use uuid::Uuid;

pub struct Cassandra {
    session: Session,
}

static CREATE_KEYSPACE_QUERY: &str = r#"
    INSERT INTO atomix.keyspaces
    (keyspace_id, name, namespace, primary_zone, secondary_zones, base_key_ranges, secondary_key_ranges)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    IF NOT EXISTS
"#;

static LIST_KEYSPACES_QUERY: &str = r#"
    SELECT keyspace_id, name, namespace, primary_zone, secondary_zones, base_key_ranges, secondary_key_ranges
    FROM atomix.keyspaces
"#;

static GET_KEYSPACE_INFO_BY_KEYSPACE_QUERY: &str = r#"
    SELECT keyspace_id, name, namespace, primary_zone, secondary_zones, base_key_ranges, secondary_key_ranges
    FROM atomix.keyspaces
    WHERE namespace = ? AND name = ?
"#;

//  TODO(kelly): Add ALLOW FILTERING is bad - discuss whether we will ever need to query by KeyspaceId in practice
//  and create an index on the field if so.
static GET_KEYSPACE_INFO_BY_KEYSPACE_ID_QUERY: &str = r#"
    SELECT keyspace_id, name, namespace, primary_zone, secondary_zones, base_key_ranges, secondary_key_ranges
    FROM atomix.keyspaces
    WHERE keyspace_id = ? ALLOW FILTERING
"#;

// TODO: Similar to tx_state_store. We should move this to a common location.
fn get_serial_query(query_text: impl Into<String>) -> Query {
    let mut query = Query::new(query_text);
    query.set_serial_consistency(Some(SerialConsistency::Serial));
    query
}

// TODO: Similar to rangeserver and tx_state_store. We should move this to a
// common location.
fn scylla_query_error_to_storage_error(qe: QueryError) -> Error {
    match qe {
        QueryError::TimeoutError | QueryError::DbError(DbError::WriteTimeout { .. }, _) => {
            Error::Timeout
        }
        _ => Error::InternalError(Some(Arc::new(qe))),
    }
}

#[derive(Debug, FromUserType, SerializeValue)]
struct SerializedRegion {
    name: String,
    cloud: Option<String>,
}

#[derive(Debug, FromUserType, SerializeValue)]
struct SerializedZone {
    region: Option<SerializedRegion>,
    name: String,
}

impl SerializedZone {
    fn from_zone(zone: Zone) -> Self {
        SerializedZone {
            name: zone.name.clone(),
            region: zone.region.map(|region| SerializedRegion {
                name: region.name.clone(),
                cloud: match region.cloud {
                    Some(proto::universe::region::Cloud::PublicCloud(cloud)) => Some(
                        proto::universe::Cloud::try_from(cloud)
                            .unwrap()
                            .as_str_name()
                            .to_string(),
                    ),
                    Some(proto::universe::region::Cloud::OtherCloud(other)) => Some(other),
                    None => None,
                },
            }),
        }
    }

    fn to_zone(self) -> Zone {
        Zone {
            name: self.name.clone(),
            region: self.region.map(|region| proto::universe::Region {
                name: region.name,
                cloud: region.cloud.map(|cloud| match cloud.as_str() {
                    "AWS" => proto::universe::region::Cloud::PublicCloud(
                        proto::universe::Cloud::Aws as i32,
                    ),
                    "AZURE" => proto::universe::region::Cloud::PublicCloud(
                        proto::universe::Cloud::Azure as i32,
                    ),
                    "GCP" => proto::universe::region::Cloud::PublicCloud(
                        proto::universe::Cloud::Gcp as i32,
                    ),
                    other => proto::universe::region::Cloud::OtherCloud(other.to_string()),
                }),
            }),
        }
    }
}

#[derive(Debug, FromUserType, SerializeValue)]
struct SerializedKeyRange {
    base_range_uuid: Uuid,
    lower_bound_inclusive: Option<Vec<u8>>,
    upper_bound_exclusive: Option<Vec<u8>>,
}

#[derive(Debug, FromUserType, SerializeValue)]
struct SerializedZonedKeyRange {
    zone: SerializedZone,
    key_range: SerializedKeyRange,
}

// Wrapper struct for the KeyspaceInfo proto.
// This is needed because the KeyspaceInfo proto is not directly convertible to
// a Scylla row. Instead, we need to implement the FromRow trait.
#[derive(Debug, FromRow, SerializeRow)]
struct SerializedKeyspaceInfo {
    keyspace_id: Uuid,
    name: String,
    namespace: String,
    primary_zone: SerializedZone,
    secondary_zones: Vec<SerializedZone>,
    base_key_ranges: Vec<SerializedKeyRange>,
    secondary_key_ranges: Vec<SerializedZonedKeyRange>,
}

impl SerializedKeyspaceInfo {
    fn construct_from_parts(
        keyspace_id: Uuid,
        name: String,
        namespace: String,
        primary_zone: Zone,
        secondary_zones: Vec<Zone>,
        base_key_range_requests: Vec<KeyRange>,
        secondary_key_ranges: Vec<ZonedKeyRange>,
    ) -> Self {
        SerializedKeyspaceInfo {
            keyspace_id,
            name,
            namespace,
            primary_zone: SerializedZone::from_zone(primary_zone),
            secondary_zones: secondary_zones
                .into_iter()
                .map(|zone| SerializedZone::from_zone(zone))
                .collect(),
            base_key_ranges: base_key_range_requests
                .into_iter()
                .map(|range| SerializedKeyRange {
                    base_range_uuid: Uuid::from_str(&range.base_range_uuid).unwrap(),
                    lower_bound_inclusive: Some(range.lower_bound_inclusive),
                    upper_bound_exclusive: Some(range.upper_bound_exclusive),
                })
                .collect(),
            secondary_key_ranges: secondary_key_ranges
                .into_iter()
                .map(|zkr| {
                    let range = zkr.key_range.unwrap();
                    SerializedZonedKeyRange {
                        zone: SerializedZone::from_zone(zkr.zone.unwrap()),
                        key_range: SerializedKeyRange {
                            base_range_uuid: Uuid::from_str(&range.base_range_uuid).unwrap(),
                            lower_bound_inclusive: Some(range.lower_bound_inclusive),
                            upper_bound_exclusive: Some(range.upper_bound_exclusive),
                        },
                    }
                })
                .collect(),
        }
    }

    /// Convert into a KeyspaceInfo proto.
    fn into_keyspace_info(self) -> KeyspaceInfo {
        let primary_zone = Zone {
            name: self.primary_zone.name,
            region: self
                .primary_zone
                .region
                .map(|region| proto::universe::Region {
                    name: region.name,
                    cloud: region.cloud.map(|cloud| match cloud.as_str() {
                        "AWS" => proto::universe::region::Cloud::PublicCloud(
                            proto::universe::Cloud::Aws as i32,
                        ),
                        "AZURE" => proto::universe::region::Cloud::PublicCloud(
                            proto::universe::Cloud::Azure as i32,
                        ),
                        "GCP" => proto::universe::region::Cloud::PublicCloud(
                            proto::universe::Cloud::Gcp as i32,
                        ),
                        other => proto::universe::region::Cloud::OtherCloud(other.to_string()),
                    }),
                }),
        };
        let base_key_ranges = self
            .base_key_ranges
            .into_iter()
            .map(|range| KeyRange {
                base_range_uuid: range.base_range_uuid.to_string(),
                lower_bound_inclusive: range.lower_bound_inclusive.unwrap_or_default(),
                upper_bound_exclusive: range.upper_bound_exclusive.unwrap_or_default(),
            })
            .collect();

        let secondary_key_ranges = self
            .secondary_key_ranges
            .into_iter()
            .map(|zkr| ZonedKeyRange {
                zone: Some(zkr.zone.to_zone()),
                key_range: Some(KeyRange {
                    base_range_uuid: zkr.key_range.base_range_uuid.to_string(),
                    lower_bound_inclusive: zkr.key_range.lower_bound_inclusive.unwrap_or_default(),
                    upper_bound_exclusive: zkr.key_range.upper_bound_exclusive.unwrap_or_default(),
                }),
            })
            .collect();
        KeyspaceInfo {
            keyspace_id: self.keyspace_id.to_string(),
            name: self.name,
            namespace: self.namespace,
            primary_zone: Some(primary_zone),
            base_key_ranges: base_key_ranges,
            secondary_zones: self
                .secondary_zones
                .into_iter()
                .map(|serialized_zone| serialized_zone.to_zone())
                .collect(),
            secondary_key_ranges: secondary_key_ranges,
        }
    }
}

impl Cassandra {
    pub async fn new(known_node: String) -> Self {
        let session = SessionBuilder::new()
            .known_node(known_node)
            .build()
            .await
            .unwrap();
        Self { session }
    }
}

impl Storage for Cassandra {
    async fn create_keyspace(
        &self,
        keyspace_id: &str,
        name: &str,
        namespace: &str,
        primary_zone: Zone,
        secondary_zones: Vec<Zone>,
        base_key_ranges: Vec<KeyRange>,
        secondary_key_ranges: Vec<ZonedKeyRange>,
    ) -> Result<String, Error> {
        // TODO: Validate base_key_ranges

        // Create a SerializedKeyspaceInfo from the input parameters
        let serialized_info = SerializedKeyspaceInfo::construct_from_parts(
            Uuid::from_str(keyspace_id).unwrap(),
            name.to_string(),
            namespace.to_string(),
            primary_zone,
            secondary_zones,
            base_key_ranges,
            secondary_key_ranges,
        );

        let keyspace_id = keyspace_id.to_string();

        // Execute the query
        let query = get_serial_query(CREATE_KEYSPACE_QUERY);
        let query_result = self
            .session
            .query_single_page(query, serialized_info, PagingState::start())
            .await
            .map_err(scylla_query_error_to_storage_error)?;

        println!("Query result: {:?}", query_result);
        // If the first row of the result is true, our insert was successful.
        // Else, insert failed, thus the keyspace already exists.
        if let Some(Some(insert_succeeded)) = query_result.0.first_row().unwrap().columns.first() {
            if !insert_succeeded.as_boolean().unwrap() {
                return Err(Error::KeyspaceAlreadyExists);
            }
        } else {
            return Err(Error::InternalError(None));
        }

        Ok(keyspace_id)
    }

    async fn list_keyspaces(
        &self,
        region: Option<proto::universe::Region>,
    ) -> Result<Vec<KeyspaceInfo>, Error> {
        // Execute the query
        let query = get_serial_query(LIST_KEYSPACES_QUERY);
        let rows = self
            .session
            .query_unpaged(query, ())
            .await
            .map_err(scylla_query_error_to_storage_error)?
            .rows
            .unwrap_or_default();

        // Convert the rows to KeyspaceInfo objects
        let keyspaces = rows
            .into_iter()
            .map(|row| {
                let serialized = row.into_typed::<SerializedKeyspaceInfo>();
                serialized
                    .map(SerializedKeyspaceInfo::into_keyspace_info)
                    .map_err(|e| Error::InternalError(Some(Arc::new(e))))
            })
            .collect::<Result<Vec<KeyspaceInfo>, Error>>()?;

        // Filter by region if provided
        // TODO: Push this to Cassandra query
        let filtered_keyspaces = if let Some(filter_region) = region {
            keyspaces
                .into_iter()
                .filter(|keyspace| {
                    keyspace
                        .primary_zone
                        .as_ref()
                        .and_then(|zone| zone.region.as_ref())
                        == Some(&filter_region)
                })
                .collect()
        } else {
            keyspaces
        };

        Ok(filtered_keyspaces)
    }

    async fn get_keyspace_info(
        &self,
        keyspace_info_search_field: KeyspaceInfoSearchField,
    ) -> Result<KeyspaceInfo, Error> {
        // Execute the query
        let rows = match keyspace_info_search_field {
            KeyspaceInfoSearchField::Keyspace { namespace, name } => {
                let query = get_serial_query(GET_KEYSPACE_INFO_BY_KEYSPACE_QUERY);
                self.session
                    .query_unpaged(query, (namespace, name))
                    .await
                    .map_err(scylla_query_error_to_storage_error)?
                    .rows
                    .unwrap_or_default()
            }
            KeyspaceInfoSearchField::KeyspaceId(keyspace_id) => {
                let query = get_serial_query(GET_KEYSPACE_INFO_BY_KEYSPACE_ID_QUERY);
                self.session
                    .query_unpaged(query, (Uuid::from_str(keyspace_id.as_str()).unwrap(),))
                    .await
                    .map_err(scylla_query_error_to_storage_error)?
                    .rows
                    .unwrap_or_default()
            }
        };

        if rows.is_empty() {
            return Err(Error::KeyspaceDoesNotExist);
        }

        if let Some(first_row) = rows.into_iter().next() {
            let serialized = first_row.into_typed::<SerializedKeyspaceInfo>();
            let keyspace_info = serialized.unwrap().into_keyspace_info();
            Ok(keyspace_info)
        } else {
            Err(Error::KeyspaceDoesNotExist)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use proto::universe::{region, Cloud, KeyRange, KeyspaceInfo, Region, Zone};

    fn create_example_keyspace_info(
        name: String,
        namespace: String,
        region_name: String,
    ) -> KeyspaceInfo {
        KeyspaceInfo {
            keyspace_id: Uuid::new_v4().to_string(),
            name,
            namespace,
            primary_zone: Some(Zone {
                region: Some(Region {
                    name: region_name,
                    cloud: Some(region::Cloud::PublicCloud(Cloud::Aws as i32)),
                }),
                name: "example_zone".to_string(),
            }),
            secondary_zones: vec![
                Zone {
                    region: Some(Region {
                        name: "example_region_1".to_string(),
                        cloud: Some(region::Cloud::PublicCloud(Cloud::Aws as i32)),
                    }),
                    name: "example_zone_1".to_string(),
                },
                Zone {
                    region: Some(Region {
                        name: "example_region_2".to_string(),
                        cloud: Some(region::Cloud::PublicCloud(Cloud::Aws as i32)),
                    }),
                    name: "example_zone_2".to_string(),
                },
            ],
            base_key_ranges: vec![
                KeyRange {
                    base_range_uuid: Uuid::new_v4().to_string(),
                    lower_bound_inclusive: vec![0, 0, 0],
                    upper_bound_exclusive: vec![128, 0, 0],
                },
                KeyRange {
                    base_range_uuid: Uuid::new_v4().to_string(),
                    lower_bound_inclusive: vec![128, 0, 0],
                    upper_bound_exclusive: vec![255, 255, 255],
                },
            ],
            secondary_key_ranges: vec![
                ZonedKeyRange {
                    zone: Some(Zone {
                        region: Some(Region {
                            name: "example_region_1".to_string(),
                            cloud: Some(region::Cloud::PublicCloud(Cloud::Aws as i32)),
                        }),
                        name: "example_zone_1".to_string(),
                    }),
                    key_range: Some(KeyRange {
                        base_range_uuid: Uuid::new_v4().to_string(),
                        lower_bound_inclusive: vec![0, 0, 0],
                        upper_bound_exclusive: vec![128, 0, 0],
                    }),
                },
                ZonedKeyRange {
                    zone: Some(Zone {
                        region: Some(Region {
                            name: "example_region_2".to_string(),
                            cloud: Some(region::Cloud::PublicCloud(Cloud::Aws as i32)),
                        }),
                        name: "example_zone_2".to_string(),
                    }),
                    key_range: Some(KeyRange {
                        base_range_uuid: Uuid::new_v4().to_string(),
                        lower_bound_inclusive: vec![128, 0, 0],
                        upper_bound_exclusive: vec![255, 255, 255],
                    }),
                },
            ],
        }
    }

    #[tokio::test]
    async fn test_local_roundtrip() {
        let original = create_example_keyspace_info(
            "example_keyspace".to_string(),
            "example_namespace".to_string(),
            "example_region".to_string(),
        );
        let serialized = SerializedKeyspaceInfo::construct_from_parts(
            Uuid::from_str(original.keyspace_id.as_str()).unwrap(),
            original.name.clone(),
            original.namespace.clone(),
            original.primary_zone.clone().unwrap(),
            original.secondary_zones.clone(),
            original.base_key_ranges.clone(),
            original.secondary_key_ranges.clone(),
        );
        let roundtrip = serialized.into_keyspace_info();
        assert!(original == roundtrip);
    }

    #[tokio::test]
    async fn test_cassandra_roundtrip() {
        let uuid_str = Uuid::new_v4().to_string();
        let example_keyspaces = vec![
            create_example_keyspace_info(
                "example_keyspace_1_".to_string() + &uuid_str,
                "example_namespace".to_string(),
                "example_region_1".to_string(),
            ),
            create_example_keyspace_info(
                "example_keyspace_2_".to_string() + &uuid_str,
                "example_namespace".to_string(),
                "example_region_2".to_string(),
            ),
        ];
        // Start a session
        let storage = Cassandra::new("127.0.0.1:9042".to_string()).await;
        let mut keyspace_ids: Vec<String> = vec![];

        for original in example_keyspaces.clone() {
            // Insert into Cassandra
            println!("Inserting keyspace: {}", original.name);
            let base_key_range_requests = original
                .base_key_ranges
                .iter()
                .map(|range| KeyRange {
                    base_range_uuid: range.base_range_uuid.clone(),
                    lower_bound_inclusive: range.lower_bound_inclusive.clone(),
                    upper_bound_exclusive: range.upper_bound_exclusive.clone(),
                })
                .collect();
            let keyspace_id = storage
                .create_keyspace(
                    &original.keyspace_id,
                    &original.name,
                    &original.namespace,
                    original.primary_zone.clone().unwrap(),
                    original.secondary_zones.clone(),
                    base_key_range_requests,
                    original.secondary_key_ranges.clone(),
                )
                .await
                .unwrap();
            keyspace_ids.push(keyspace_id.clone());
            // Print keyspace id
            println!("Keyspace ID: {}", keyspace_id);
            // List keyspaces from Cassandra
            let keyspaces = storage.list_keyspaces(None).await.unwrap();
            // Find the keyspace we inserted by id
            let roundtrip = keyspaces
                .into_iter()
                .find(|k| k.keyspace_id == keyspace_id)
                .unwrap_or_else(|| panic!("Keyspace with id {} was not found", keyspace_id));

            assert!(original == roundtrip);
        }

        let this_region_keyspace_id = keyspace_ids[0].clone();
        let other_region_keyspace_id = keyspace_ids[1].clone();
        // Test list_keyspaces with region filter
        let keyspaces = storage
            .list_keyspaces(Some(Region {
                name: "example_region_1".to_string(),
                cloud: Some(region::Cloud::PublicCloud(Cloud::Aws as i32)),
            }))
            .await
            .unwrap();
        // The list should NOT contain the keyspace of the other region
        assert!(!keyspaces
            .iter()
            .any(|k| k.keyspace_id == other_region_keyspace_id));
        // The list should contain the keyspace of this region
        assert!(keyspaces
            .iter()
            .any(|k| k.keyspace_id == this_region_keyspace_id));

        // Try to create the same keyspace again and expect an error
        let keyspace = example_keyspaces[0].clone();
        let result = storage
            .create_keyspace(
                &keyspace.keyspace_id,
                &keyspace.name,
                &keyspace.namespace,
                keyspace.primary_zone.unwrap(),
                keyspace.secondary_zones,
                keyspace.base_key_ranges,
                keyspace.secondary_key_ranges,
            )
            .await;
        assert!(matches!(result, Err(Error::KeyspaceAlreadyExists)));
    }
}
