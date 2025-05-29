CREATE TABLE range_map (
    keyspace_id                  uuid,
    range_id                     uuid,
    range_type                   text,
    key_lower_bound_inclusive    blob,
    key_upper_bound_exclusive    blob,
    assignee                     text,
    PRIMARY KEY  ((keyspace_id), range_id)
  ) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TABLE epoch (
    region      text,
    epoch       bigint,
    timestamp   timestamp,
    PRIMARY KEY  (region)
  ) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TYPE epoch_range (
  lower_bound_inclusive    bigint,
  upper_bound_inclusive    bigint
);

CREATE TABLE range_leases (
  range_id                     uuid,
  range_type                   text,
  key_lower_bound_inclusive    blob,
  key_upper_bound_exclusive    blob,
  leader_sequence_number       bigint,
  epoch_lease                  epoch_range,
  safe_snapshot_epochs         epoch_range,
  // Secondary-only fields
  applied_secondary_offset     bigint,
  PRIMARY KEY  (range_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  };

CREATE TABLE records (
    range_id           uuid,
    key                blob,
    epoch              bigint,
    value              blob,
    is_tombstone       boolean,
    PRIMARY KEY  ((range_id), key, epoch)
) WITH CLUSTERING ORDER BY (key ASC, epoch DESC)
  AND COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};

CREATE TABLE wal (
    wal_id          uuid,
    first_offset    bigint,
    next_offset     bigint,
    offset          bigint,
    write_id        uuid,
    content         blob,
    PRIMARY KEY  ((wal_id), offset)
) WITH CLUSTERING ORDER BY (offset ASC)
  AND COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};

CREATE TABLE transactions (
    transaction_id    uuid,
    status            text,
    epoch             bigint,
    commit_info       blob,
    PRIMARY KEY  (transaction_id)
) WITH COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};

CREATE TYPE region (
  name     text,
  cloud    text
);

CREATE TYPE zone (
  region   frozen<region>,
  name     text
);

CREATE TYPE key_range (
  base_range_uuid    uuid,
  lower_bound_inclusive    blob,
  upper_bound_exclusive    blob
);

CREATE TYPE zoned_key_range (
  zone                   frozen<zone>,
  key_range              frozen<key_range>
);

CREATE TABLE keyspaces (
    keyspace_id          uuid,
    namespace            text,
    name                 text,
    primary_zone         zone,
    secondary_zones      list<frozen<zone>>,
    base_key_ranges      list<frozen<key_range>>,
    secondary_key_ranges list<frozen<zoned_key_range>>,
    PRIMARY KEY ((namespace), name)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};
