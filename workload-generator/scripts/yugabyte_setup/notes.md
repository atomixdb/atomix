```sh
# Based on:
# https://docs.yugabyte.com/preview/deploy/multi-dc/async-replication/async-transactional-setup-manual/

DB=kv_store
PRIMARY_MASTER="172.18.0.2"
SECONDARY_MASTER="172.18.0.3"

# On the secondary
./bin/yb-admin \
    --master_addresses $SECONDARY_MASTER \
    create_snapshot_schedule 1 10 ysql.yugabyte

# On the primary
# - Get table ID
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



```
