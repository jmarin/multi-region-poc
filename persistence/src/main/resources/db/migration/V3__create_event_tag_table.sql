-- Event tag table for Pekko Persistence JDBC
-- Used for tag-based event queries and projections
CREATE TABLE IF NOT EXISTS event_tag (
    event_id BIGINT NOT NULL,
    tag VARCHAR(255) NOT NULL,
    PRIMARY KEY(event_id, tag),
    CONSTRAINT fk_event_journal
        FOREIGN KEY(event_id)
        REFERENCES event_journal(ordering)
        ON DELETE CASCADE
);

-- Index for tag lookups
CREATE INDEX IF NOT EXISTS event_tag_tag_idx ON event_tag(tag);
