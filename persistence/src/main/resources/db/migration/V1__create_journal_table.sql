-- Journal table for Pekko Persistence JDBC
-- Stores all events for event sourcing
CREATE TABLE IF NOT EXISTS event_journal (
    ordering BIGSERIAL NOT NULL,
    persistence_id VARCHAR(255) NOT NULL,
    sequence_number BIGINT NOT NULL,
    deleted BOOLEAN DEFAULT FALSE NOT NULL,
    tags VARCHAR(255),
    message BYTEA NOT NULL,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(persistence_id, sequence_number)
);

-- Index for ordering column (used for queries)
CREATE UNIQUE INDEX IF NOT EXISTS event_journal_ordering_idx ON event_journal(ordering);

-- Index for tags (used for tag-based queries)
CREATE INDEX IF NOT EXISTS event_journal_tags_idx ON event_journal USING gin(string_to_array(tags, ','));
