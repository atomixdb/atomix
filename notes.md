### Range Assignment Flow

1. Client creates new Keyspace in Universe.
2. Warden polls the universe and pick up on the new keyspace and its ranges.
    - Initial keyspace ranges exist in the `KeyspaceInfo` table.
3. Warden steams updates to RangeServers.
    - The Warden saves ranges and their desired assignee in the `range_map` table.
    - The Warden also initializes the leases in the `range_lease` table but
      doesn't assign them yet. They will be claimed by the RangeServers.
    - Each update is a `WardenUpdate` message. It only contains the range_id
      and whether to load/unload the range.
4. RangeServers load the assigned ranges.
    - To get ownership, they write to the `range_lease` table to update the
      leader_sequence_number and the epoch_range for which the lease is valid.


For secondary ranges, the RangeServer **needs** to know it's secondary. Why?
Because the secondary data-path might be different from the primary data-path.


### RangeServer Architecture

Each RangeServer owns a bunch of ranges (see assignment flow above).
- Each range is associated with a `RangeManager`.
- The RangeServer receives updates from the Warden and creates `RangeManager`
  objects accordingly.

Questions:
- What do we replicate? Only the commits?
- Need to add new endpoints for replication traffic. Do we use the fast network?
  What about HoL blocking with primary messages?
- How much should we buffer at the primary, if at all? Should we just re-read
  everything from Cassandra?

### APIs

#### Universe

KeyspaceInfo:
- secondary_zones
- secondary_ranges

#### Warden

range_leases (Cassandra):
- range_type

#### RangeServer

WardenUpdate (proto from warden):
- Send the range type as well so that the rangeserver knows how to load the
  range
- replication_mapping: new field
    - list of (primary_range, secondary_range, assignee) tupples
    - converted to NewReplicationMapping struct and sent over channel
- Handle primary and secondary ranges separately:
    - Introduce SecondaryRangeManagerTrait with methods specific to secondary
      ranges.

#### Replication Protocol

Init Phase:
1. Primary sends ReplicationInitRequest for a specific secondary range_id
2. Secondary sends ReplicationInitResponse specifying the last replicated offset
3. Primary starts steaming ReplicationDataRequests
4. Secondary sends ReplicationDataResponse back periodically