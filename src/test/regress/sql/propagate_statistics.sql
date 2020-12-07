CREATE SCHEMA "statistics'test";

SET search_path TO "statistics'test";
SET citus.next_shard_id TO 980000;
SET client_min_messages TO WARNING;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

-- test create statistics propagation
CREATE TABLE test_stats (
    a   int,
    b   int
);

SELECT create_distributed_table('test_stats', 'a');

-- SET citus.log_remote_commands TO true;
CREATE STATISTICS s1 (dependencies) ON a, b FROM test_stats;

DROP TABLE test_stats;
DROP SCHEMA "statistics'test";
