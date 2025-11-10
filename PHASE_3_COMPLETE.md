# Phase 3: PostgreSQL Integration with Flyway - Complete

## Summary

Phase 3 has been successfully completed. PostgreSQL persistence has been integrated with Flyway database migrations, supporting three regional databases for the multi-datacenter architecture.

## Files Created

### Persistence Module (`persistence/src/main/scala/com/jmarin/filemanager/persistence/`)

1. **DatabaseMigration.scala** - Flyway migration utility
   - Reads database configuration from Typesafe Config (`slick.db.*`)
   - Creates and configures Flyway instance
   - Executes migrations and returns migration count
   - Comprehensive error handling and logging

2. **MigrationRunner.scala** - CLI tool for running migrations
   - Runs migrations for all three regions (us-east-1, eu-west-1, ap-south-1)
   - Region-specific configuration loading
   - Success/failure reporting per region
   - Overall migration status summary

### Flyway Migration Scripts (`persistence/src/main/resources/db/migration/`)

3. **V1__create_journal_table.sql** - Event journal table
   - Stores all persisted events with ordering, persistence_id, sequence_number
   - Tags array for event filtering and replication
   - Timestamp for event tracking
   - Indexes:
     - Primary key on ordering
     - Unique index on (persistence_id, sequence_number)
     - GIN index on tags for fast tag queries

4. **V2__create_snapshot_table.sql** - Snapshot table
   - Stores actor state snapshots for faster recovery
   - Fields: persistence_id, sequence_number, created timestamp, snapshot bytea
   - Primary key on (persistence_id, sequence_number)
   - Index on created timestamp for maintenance queries

5. **V3__create_event_tag_table.sql** - Event tag table
   - Separate table for tag-based event queries
   - Foreign key reference to event_journal(ordering)
   - Composite primary key on (event_id, tag)
   - Index on tag for fast tag lookups

### Configuration Files (Duplicated in both `api` and `persistence` modules)

6. **Region-Specific Configurations**
   - `us-east-1.conf`: Port 5433, database filemanager_us_east_1
   - `eu-west-1.conf`: Port 5434, database filemanager_eu_west_1
   - `ap-south-1.conf`: Port 5435, database filemanager_ap_south_1
   - All use common credentials: username `filemanager`, password `filemanager123`

### Test Files (`persistence/src/test/scala/com/jmarin/filemanager/persistence/`)

7. **DatabaseMigrationSpec.scala** - Unit tests for DatabaseMigration
   - Tests migration file discovery (3 migration files expected)
   - Tests config path parsing (slick.db.*)
   - Validates Flyway setup without running actual migrations

## Build Configuration Updates

### Dependencies Added to `build.sbt`

```scala
"org.flywaydb" % "flyway-core" % "10.21.0",
"org.flywaydb" % "flyway-database-postgresql" % "10.21.0"
```

## Docker Infrastructure Updates

### PostgreSQL Port Configuration

Refactored from default ports to avoid conflicts:
- **us-east-1**: Port 5433 (was 5432)
- **eu-west-1**: Port 5434 (was 5433)
- **ap-south-1**: Port 5435 (was 5434)

All containers use PostgreSQL 15 with:
- Health checks via `pg_isready`
- Persistent volumes for data storage
- Isolated networks per region

## Database Schema

### event_journal Table
```sql
- ordering: BIGSERIAL PRIMARY KEY (global event ordering)
- deleted: BOOLEAN (soft delete marker)
- persistence_id: VARCHAR(255) (entity identifier)
- sequence_number: BIGINT (per-entity sequence)
- writer: VARCHAR(255) (writer identifier)
- write_timestamp: BIGINT (event timestamp)
- adapter_manifest: VARCHAR(255) (serialization manifest)
- event_payload: BYTEA (serialized event)
- event_ser_id: INTEGER (serializer ID)
- event_ser_manifest: VARCHAR(255) (event type manifest)
- meta_payload: BYTEA (optional metadata)
- meta_ser_id: INTEGER (metadata serializer)
- meta_ser_manifest: VARCHAR(255) (metadata manifest)
- tags: TEXT[] (event tags for filtering)
```

### snapshot Table
```sql
- persistence_id: VARCHAR(255) (entity identifier)
- sequence_number: BIGINT (snapshot sequence)
- created: BIGINT (snapshot timestamp)
- snapshot_ser_id: INTEGER (serializer ID)
- snapshot_ser_manifest: VARCHAR(255) (snapshot type)
- snapshot_payload: BYTEA (serialized snapshot)
- meta_payload: BYTEA (optional metadata)
- meta_ser_id: INTEGER (metadata serializer)
- meta_ser_manifest: VARCHAR(255) (metadata manifest)
```

### event_tag Table
```sql
- event_id: BIGINT (FK to event_journal.ordering)
- tag: VARCHAR(255) (tag value)
- PRIMARY KEY (event_id, tag)
```

## Migration Execution Results

Successfully ran migrations on all three regional databases:

```
✓ us-east-1: Applied 3 migrations
  - V1__create_journal_table.sql
  - V2__create_snapshot_table.sql
  - V3__create_event_tag_table.sql

✓ eu-west-1: Applied 3 migrations
  - V1__create_journal_table.sql
  - V2__create_snapshot_table.sql
  - V3__create_event_tag_table.sql

✓ ap-south-1: Applied 3 migrations
  - V1__create_journal_table.sql
  - V2__create_snapshot_table.sql
  - V3__create_event_tag_table.sql

Migration completed for all regions
```

## Test Results

```
DatabaseMigrationSpec:
✓ should load migration files from classpath
✓ should use correct config path

Total: 2 tests passed, 0 failed
```

Overall project test suite: **10 tests passed** (8 from Phase 2 + 2 from Phase 3)

## PostgreSQL Connection Parameters

For connecting with pgAdmin4 or other database tools:

### US-EAST-1
- Host: `localhost`
- Port: `5433`
- Database: `filemanager_us_east_1`
- Username: `filemanager`
- Password: `filemanager123`

### EU-WEST-1
- Host: `localhost`
- Port: `5434`
- Database: `filemanager_eu_west_1`
- Username: `filemanager`
- Password: `filemanager123`

### AP-SOUTH-1
- Host: `localhost`
- Port: `5435`
- Database: `filemanager_ap_south_1`
- Username: `filemanager`
- Password: `filemanager123`

## Key Design Decisions

1. **Flyway for Migrations**: Industry-standard migration tool with version control and rollback support
2. **JDBC Plugin Compatibility**: Tables designed to work with pekko-persistence-jdbc 1.1.1 native Scala 3 version
3. **Tag-Based Replication**: Event tags enable selective event replication across regions
4. **Separate Tag Table**: Optimized for tag-based queries without JSON operations
5. **Regional Isolation**: Each region has its own database for independence and failure isolation
6. **Configuration Duplication**: Config files in both api and persistence modules for CLI tool access
7. **Non-Standard Ports**: Ports 5433-5435 to avoid conflicts with existing PostgreSQL installations

## Technical Highlights

- **Pekko Persistence JDBC**: Native Scala 3 support (version 1.1.1)
- **Flyway**: Latest version 10.21.0 with PostgreSQL-specific driver
- **PostgreSQL**: Version 15 with full event sourcing schema
- **Migration Runner**: Automated tool to initialize all regional databases
- **Config Management**: Typesafe Config with environment-specific overrides

## Infrastructure Verification

All Docker services verified healthy:
```
✓ filemanager-postgres-us-east-1: UP (port 5433)
✓ filemanager-postgres-eu-west-1: UP (port 5434)
✓ filemanager-postgres-ap-south-1: UP (port 5435)
✓ filemanager-minio-us-east-1: UP (ports 9000/9100)
✓ filemanager-minio-eu-west-1: UP (ports 9001/9101)
✓ filemanager-minio-ap-south-1: UP (ports 9002/9102)
```

## Next Steps

Phase 3 is complete. Ready to proceed with:

- **Phase 4**: MinIO Storage Integration
  - Implement StorageService for S3-compatible operations
  - File upload/download to MinIO
  - Checksum verification
  - Multi-region file replication

- **Phase 5**: gRPC Service Implementation
  - Define protobuf messages
  - Implement gRPC service handlers
  - Integrate with FileManager actor via cluster sharding

- **Phase 6**: REST API Implementation
  - HTTP endpoints for file operations
  - Multipart file upload support
  - Integration with gRPC service

- **Phase 7**: Multi-DC Configuration
  - Configure Replicated Event Sourcing transport
  - Set up replica IDs for all regions
  - Test cross-DC event replication
