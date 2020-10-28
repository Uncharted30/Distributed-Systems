# Fault tolerant key/value storage system

- Raft as consensus algorithm
- Key/value storage system provides strong consistency
- Performance improvement: log quick rollback, log compaction and state snapshotting
- Keys are sharded over a set of replica groups, so that system throughput would increase in proportion to the number of groups
