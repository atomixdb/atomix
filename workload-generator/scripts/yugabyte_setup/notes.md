```sh
# Based on:
# https://docs.yugabyte.com/preview/deploy/multi-dc/async-replication/async-transactional-setup-manual/

DB=kv_store
PRIMARY_MASTER="172.18.0.2"
SECONDARY_MASTER="172.18.0.3"

# On the primary, create the table
ysqlsh -h $PRIMARY_MASTER
# CREATE TABLE kv_store (
#     k TEXT,
#     v TEXT,
#     PRIMARY KEY (k ASC)
# ) SPLIT AT VALUES (('d'), ('m'), ('t'));


# On the secondary
./bin/yb-admin \
    --master_addresses $SECONDARY_MASTER \
    create_snapshot_schedule 1 10 ysql.yugabyte

# On the primary
# - Get table ID
# Example output: yugabyte.kv_store [ysql_schema=public] [000034cb000030008000000000004000]
yb-admin --master_addresses $PRIMARY_MASTER list_tables | grep $DB
TABLE_ID=000034cb000030008000000000004000

# - Get the bootstrap ID
yb-admin \
    --master_addresses $PRIMARY_MASTER \
    bootstrap_cdc_producer $TABLE_ID
# Example output:
# table id: 000034cb000030008000000000004000, CDC bootstrap id: de0b5affa1d0a6acbd42ab48a22e653f
BOOTSTRAP_ID=de0b5affa1d0a6acbd42ab48a22e653f

# On the secondary
yb-admin \
    --master_addresses $SECONDARY_MASTER \
    setup_universe_replication \
    kv_store_replication \
    $PRIMARY_MASTER \
    $TABLE_ID \
    $BOOTSTRAP_ID \
    transactional

# This is likely not needed but keeping it cause the docs have it
yb-admin \
    --master_addresses $SECONDARY_MASTER \
    change_xcluster_role STANDBY


# Get status
yb-admin \
    --master_addresses $(hostname -i) \
    get_replication_status kv_store_replication

yb-admin \
    --master_addresses $(hostname -i) \
    get_xcluster_safe_time \
    include_lag_and_skew


# If want to delete:
yb-admin \
    -master_addresses $SECONDARY_MASTER \
    delete_universe_replication \
    kv_store_replication


# Performing the failover
# Source: https://docs.yugabyte.com/preview/deploy/multi-dc/async-replication/async-transactional-failover

# First, stop application sending requests

# On the secondary

# Pause replication
# Expected output: "Replication disabled successfully"
./bin/yb-admin \
    -master_addresses $SECONDARY_MASTER \
    set_universe_replication_enabled kv_store_replication 0

# Get the safe time
./bin/yb-admin \
    -master_addresses $SECONDARY_MASTER \
    get_xcluster_safe_time include_lag_and_skew

SAFE_TIME="2025-06-23 22:00:05.369524"

# Find snapshot schedule
./bin/yb-admin \
    -master_addresses $SECONDARY_MASTER \
    list_snapshot_schedules

SCHEDULE_ID="4bfadc1e-c423-409a-b477-469c0c68c35b"

# Restore to safe_time
./bin/yb-admin \
    -master_addresses $SECONDARY_MASTER \
    restore_snapshot_schedule $SCHEDULE_ID $SAFE_TIME

# Verify
# Example output:
# {
#     "restorations": [
#         {
#             "id": "a3fdc1c0-3e07-4607-91a7-1527b7b8d9ea",
#             "snapshot_id": "3ecbfc16-e2a5-43a3-bf0d-82e04e72be65",
#             "state": "RESTORED"
#         }
#     ]
# }
./bin/yb-admin \
    -master_addresses $SECONDARY_MASTER \
    list_snapshot_restorations

# Delete old replication group
./bin/yb-admin \
    -master_addresses $SECONDARY_MASTER \
    delete_universe_replication kv_store_replication

```
