-- Snapshot table for Pekko Persistence JDBC
-- Stores snapshots of actor state for faster recovery
CREATE TABLE IF NOT EXISTS snapshot (
    persistence_id VARCHAR(255) NOT NULL,
    sequence_number BIGINT NOT NULL,
    created BIGINT NOT NULL,
    snapshot BYTEA NOT NULL,
    PRIMARY KEY(persistence_id, sequence_number)
);

-- Index for created timestamp (used for cleanup)
CREATE INDEX IF NOT EXISTS snapshot_created_idx ON snapshot(created);
