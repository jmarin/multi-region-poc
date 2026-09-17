# Multi-Region File Manager POC

> **Active-Active Multi-Region Distributed Event Sourced System**  
> **Runs Completely Locally for Demonstration**

A proof-of-concept distributed File Manager system built with Apache Pekko, demonstrating multi-datacenter capabilities using Replicated Event Sourcing on PostgreSQL. This project is designed to run entirely on your local machine using Docker Compose, simulating 3 independent datacenters.

## Overview

This project showcases an **active-active multi-region architecture** where:
- **Multi-DC Event Sourcing**: Replicated Event Sourcing across multiple datacenters with independent write capabilities
- **Active-Active Topology**: All datacenters actively accept writes simultaneously - no passive/standby nodes
- **Eventual Consistency**: Conflict-free data replication with deterministic conflict resolution across regions
- **Distributed File Management**: File uploads/downloads to S3 with metadata persistence in each region
- **High Availability**: Multi-master writes with automatic failover and zero downtime

## Architecture

- **Framework**: Apache Pekko (Actor model, Clustering, Event Sourcing)
- **Language**: Scala 3.x
- **Serialization**: CBOR (Concise Binary Object Representation) for event persistence
- **Persistence**: PostgreSQL with pekko-persistence-jdbc
- **Storage**: MinIO (S3-compatible) for local development and demonstration
- **Internal API**: gRPC (Pekko gRPC) - Event sourcing service layer
- **Public API**: REST (Http4s + Tapir, Cats Effect) - Client-facing endpoints that call gRPC service

**Layered Architecture:**
```
External Clients
      ↓
  REST API (Http4s + Tapir) - Multipart uploads, JSON responses
      ↓
  gRPC Service (Protobuf) - Event sourcing operations
      ↓
  Event Sourced Actors (CBOR serialization)
      ↓
  PostgreSQL (CBOR-serialized events)
```

**Local Environment:**
- 3 Pekko nodes (localhost:8081, 8082, 8083)
- 3 PostgreSQL instances (localhost:5433-5435)
- 3 MinIO instances (localhost:9000-9002)

## Domain Model

The system manages file lifecycle operations:
- Upload files with metadata (name, size, MIME type, checksum)
- Download files via presigned URLs
- Delete files (removes current state; the delete event remains in the event log)
- Query file metadata across datacenters

Each operation is captured as an event and replicated across all datacenters, ensuring eventual consistency.

### Data Residency & Regional Restrictions (not yet implemented)

The gRPC/REST contracts carry an `owner` field and an `allowedRegions` field on file metadata, intended to support per-file regional upload restrictions and ownership tracking for compliance (GDPR, data sovereignty, etc.). These fields are **not currently tracked in the domain model** (`core`/`persistence`) — they are accepted on requests but always returned as empty (`allowedRegions = Seq.empty`, `owner = ""`), and no restriction is enforced on upload or download. This is planned but unimplemented.

## Project Structure

```
multi-region-poc/
├── core/           # Domain model, commands, events, state
├── persistence/    # Event sourcing, database integration, CBOR serialization
├── grpc/           # gRPC service layer (internal API for event sourcing; also holds filemanager.proto)
├── endpoints/      # Tapir endpoint definitions shared by the REST API
├── api/            # REST API (Http4s + Tapir), public API that calls the gRPC service
├── storage/        # MinIO/S3 integration
├── protocol/       # Reserved for shared protobuf definitions (currently unused/empty)
├── integration/    # End-to-end tests
└── docker-compose.yml  # Docker Compose for local multi-DC setup (PostgreSQL + MinIO)
```

## Getting Started

See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) for detailed implementation steps.

### Prerequisites

**All you need is Docker!**
- Docker Desktop (or Docker Engine + Docker Compose)
- JDK 11 or higher
- sbt 1.9+
- 8GB RAM recommended for running 3 DCs simultaneously

**No cloud account required** - everything runs locally using MinIO (S3-compatible storage).

### Quick Start

1. **Clone and setup**
   ```bash
   git clone <repository-url>
   cd multi-region-poc
   ```

2. **Start complete multi-DC infrastructure**
   ```bash
   # Start PostgreSQL + MinIO for 3 datacenters
   docker-compose up -d
   
   # Wait for services to initialize
   sleep 30
   
   # Initialize MinIO buckets
   ./scripts/init-minio-buckets.sh
   ```

3. **Run the application (3 datacenters)**
   ```bash
   # Terminal 1 - DC1 (us-east-1)
   # Runs gRPC service on :9090 and REST API on :8081
   sbt "project api" "run -Dconfig.file=api/src/main/resources/us-east-1.conf"
   
   # Terminal 2 - DC2 (eu-west-1)
   # Runs gRPC service on :9091 and REST API on :8082
   sbt "project api" "run -Dconfig.file=api/src/main/resources/eu-west-1.conf"
   
   # Terminal 3 - DC3 (ap-south-1)
   # Runs gRPC service on :9092 and REST API on :8083
   sbt "project api" "run -Dconfig.file=api/src/main/resources/ap-south-1.conf"
   ```

4. **Test multi-region replication**
   ```bash
   # Upload a file to DC1 (substitute any local file you have on hand)
   curl -X POST http://localhost:8081/api/files \
     -F "file=@./README.md" \
     -F "owner=user@example.com"
   # Response: {"fileId": "abc-123", ...}
   # Note: "owner" is accepted by the API but not yet persisted in the domain
   # model - it will come back empty on subsequent reads (see "Data Residency
   # & Regional Restrictions" above).
   
   # Query from DC2 (different datacenter!)
   sleep 3
   curl http://localhost:8082/api/files/abc-123
   
   # Download from DC3
   curl http://localhost:8083/api/files/abc-123/download
   ```

5. **Test gRPC service directly (optional)**
   ```bash
   # Install grpcurl
   go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
   
   # List available services
   grpcurl -plaintext localhost:9090 list
   
   # Call gRPC service directly
   grpcurl -plaintext -d '{
     "file_id": "test-123",
     "name": "test.pdf",
     "s3_key": "files/test-123",
     "size_bytes": 1024,
     "mime_type": "application/pdf",
     "owner": "user@example.com"
   }' localhost:9090 com.jmarin.filemanager.grpc.FileManagerService/UploadFile
   ```

6. **Explore with MinIO Console**
   ```bash
   # Open MinIO web consoles
   open http://localhost:9100  # DC1
   open http://localhost:9101  # DC2
   open http://localhost:9102  # DC3
   # Login: minioadmin / minioadmin123
   ```

## Key Features

### Active-Active Multi-Region Architecture
- **No Single Point of Failure**: All regions are active and can serve traffic independently
- **Geographic Distribution**: Deploy across multiple cloud regions (US, EU, Asia, etc.)
- **Regional Autonomy**: Each datacenter operates independently and continues functioning during network partitions
- **Multi-Master Writes**: All regions accept writes concurrently without coordination

### Replicated Event Sourcing
- Events are persisted locally and replicated asynchronously to other datacenters
- Each DC maintains its own event journal in PostgreSQL
- Automatic conflict resolution using replica ID ordering
- Event causality preserved across regions

### Multi-Datacenter Support
- Configure multiple replica IDs (e.g., dc-us-east-1, dc-eu-west-1, dc-ap-south-1)
- Low-latency local reads from nearest datacenter
- Asynchronous cross-region event replication
- Zero-downtime upgrades and maintenance per region

### Fault Tolerance
- Survives datacenter failures
- Automatic recovery when partitions heal
- No data loss with proper replication

## Implementation Phases

1. ✅ **Project Setup** - sbt configuration, dependencies
2. ✅ **Domain Model** - Event sourced FileManager entity
3. ✅ **PostgreSQL Integration** - Persistence layer
4. ✅ **S3 Storage** - File upload/download
5. ✅ **HTTP API** - REST endpoints
6. ✅ **Multi-DC Configuration** - Cluster setup
7. ✅ **Testing** - Unit, integration, and scenario tests
8. ⏳ **Observability** - Structured logging and a basic `/health` endpoint exist; metrics/tracing are not yet implemented

See `PHASE_2_COMPLETE.md` through `PHASE6_COMPLETE.md` and `NEXT_STEPS_COMPLETE.md` for the detailed history of each completed phase. Regional upload restrictions (see above) remain unimplemented despite being part of the original domain model design.

## Documentation

- [Implementation Plan](IMPLEMENTATION_PLAN.md) - Detailed technical plan
- [Architecture Decisions](docs/architecture.md) - Design rationale (TBD)
- [API Documentation](docs/api.md) - REST API reference (TBD)

## Contributing

This is a proof-of-concept project. Contributions and feedback are welcome!

## License

See [LICENSE](LICENSE) file for details.