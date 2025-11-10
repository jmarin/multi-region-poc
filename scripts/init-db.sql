-- File Manager Database Initialization Script
-- This script creates the necessary tables for Pekko Persistence JDBC

-- Event Journal table
CREATE TABLE IF NOT EXISTS event_journal(
  ordering BIGSERIAL,
  persistence_id VARCHAR(255) NOT NULL,
  sequence_number BIGINT NOT NULL,
  deleted BOOLEAN DEFAULT FALSE NOT NULL,
  tags VARCHAR(255) DEFAULT NULL,
  message BYTEA NOT NULL,
  created TIMESTAMP NOT NULL,
  PRIMARY KEY(persistence_id, sequence_number)
);

CREATE UNIQUE INDEX IF NOT EXISTS event_journal_ordering_idx ON event_journal(ordering);

-- Snapshot table
CREATE TABLE IF NOT EXISTS snapshot(
  persistence_id VARCHAR(255) NOT NULL,
  sequence_number BIGINT NOT NULL,
  created BIGINT NOT NULL,
  snapshot BYTEA NOT NULL,
  PRIMARY KEY(persistence_id, sequence_number)
);

-- Tag scanning table for read-side projections
CREATE TABLE IF NOT EXISTS event_tag(
  event_id BIGSERIAL,
  tag VARCHAR(255) NOT NULL,
  PRIMARY KEY(event_id, tag)
);

-- Metadata table for tracking read-side projection progress
CREATE TABLE IF NOT EXISTS projection_offset(
  projection_name VARCHAR(255) NOT NULL,
  projection_key VARCHAR(255) NOT NULL,
  current_offset VARCHAR(255) NOT NULL,
  manifest VARCHAR(4) NOT NULL,
  mergeable BOOLEAN NOT NULL,
  last_updated BIGINT NOT NULL,
  PRIMARY KEY(projection_name, projection_key)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS event_journal_persistence_id_idx ON event_journal(persistence_id);
CREATE INDEX IF NOT EXISTS event_journal_created_idx ON event_journal(created);
CREATE INDEX IF NOT EXISTS snapshot_persistence_id_idx ON snapshot(persistence_id);

-- Grant permissions (if needed for specific user)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO filemanager;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO filemanager;

-- Display success message
SELECT 'Database initialized successfully!' AS status;
