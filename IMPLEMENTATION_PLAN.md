# Multi-DC File Manager Implementation Plan
## Pekko Replicated Event Sourcing on PostgreSQL

---

## 1. Project Overview

### 1.1 Objective
Build a distributed File Manager system that demonstrates Pekko's multi-datacenter capabilities using Replicated Event Sourcing, backed by PostgreSQL, with S3-compatible storage for file content. **This project is designed to run entirely locally using Docker Compose for demonstration and testing purposes.**

### 1.2 Core Features
- **File Upload**: Upload files to S3-compatible storage with metadata persistence
- **Regional Restrictions**: Enforce data residency requirements per file
  - Unrestricted uploads (default): File can go to any datacenter
  - Region-restricted uploads: File must be uploaded to specified regions only
  - Upload validation: Reject uploads to unauthorized regions
- **File Download**: Retrieve files from S3-compatible storage with region validation
  - Download authorization: Verify requesting region is allowed
  - Descriptive errors: Clear messages when download is denied
- **Metadata Management**: Track file size, MIME type, upload timestamp, owner, allowed regions
- **Multi-DC Replication**: Event replication across multiple datacenters
- **Eventual Consistency**: Conflict-free replicated data across regions
- **Local Deployment**: Fully containerized environment for local demonstration

### 1.3 Technology Stack
- **Framework**: Apache Pekko (Akka fork)
- **Language**: Scala 3.x
- **Build Tool**: sbt
- **Event Sourcing**: Pekko Persistence with Replicated Event Sourcing
- **Database**: PostgreSQL (with JDBC plugin)
- **Object Storage**: MinIO (S3-compatible, for local development and demonstration)
- **Alternative Storage**: LocalStack (optional S3-compatible alternative)
- **Serialization**: CBOR (Concise Binary Object Representation) via Jackson
- **Internal API**: gRPC (Pekko gRPC) - Event sourcing service layer
- **Public API**: REST via Pekko HTTP - Client-facing endpoints
- **UI (Optional)**: React - Web-based file manager interface with drag & drop
- **Containerization**: Docker & Docker Compose
- **Testing**: ScalaTest, Pekko TestKit

---

## 2. Architecture Design

### 2.1 System Components

#### 2.1.1 Domain Model
```
FileEntity (Event Sourced Entity)
├── FileId (unique identifier)
├── Commands
│   ├── UploadFile(fileId, name, content, mimeType, owner, allowedRegions)
│   ├── DeleteFile(fileId)
│   ├── UpdateMetadata(fileId, metadata)
│   ├── GetFileInfo(fileId)
│   └── RequestDownload(fileId, requestingRegion)
├── Events
│   ├── FileUploaded(fileId, name, size, mimeType, s3Key, owner, timestamp, replicaId, allowedRegions)
│   ├── FileDeleted(fileId, timestamp, replicaId)
│   ├── MetadataUpdated(fileId, metadata, timestamp, replicaId)
│   ├── DownloadAuthorized(fileId, region, timestamp)
│   └── DownloadDenied(fileId, region, reason, timestamp)
└── State
    ├── FileMetadata(name, size, mimeType, s3Key, owner, created, modified)
    ├── AllowedRegions (Set[String]) - empty means unrestricted
    └── Status (Active, Deleted)
```

#### 2.1.2 Core Services
1. **FileManager Actor**: Replicated Event Sourced entity managing file lifecycle with region validation
2. **FileManagerGrpcService**: gRPC service exposing event sourcing operations with regional enforcement
3. **RestApiService**: HTTP/REST layer that calls gRPC service and handles region-specific errors
4. **S3StorageService**: Interface for MinIO operations (upload, download, delete)
5. **FileUploadHandler**: Multipart file upload processing via REST, validates regions, stores in MinIO, calls gRPC
6. **FileQueryService**: Read-side projection for querying file metadata
7. **RegionValidator**: Validates upload/download requests against allowed regions
8. **ReplicationCoordinator**: Manages cross-DC replication topology

### 2.2 Multi-DC Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Applications                      │
└──────────────────┬────────────────────┬─────────────────────┘
                   │                    │
         ┌─────────▼─────────┐  ┌──────▼──────────┐
         │   DC-1 (US-East)  │  │  DC-2 (EU-West) │
         │                   │  │                  │
         │  ┌─────────────┐  │  │  ┌────────────┐ │
         │  │  REST API   │  │  │  │  REST API  │ │
         │  │ (Pekko HTTP)│  │  │  │(Pekko HTTP)│ │
         │  └──────┬──────┘  │  │  └─────┬──────┘ │
         │         │ gRPC    │  │        │ gRPC   │
         │  ┌──────▼──────┐  │  │  ┌─────▼──────┐ │
         │  │gRPC Service │  │  │  │gRPC Service│ │
         │  │(FileManager)│  │  │  │(FileManager)│ │
         │  └──────┬──────┘  │  │  └─────┬──────┘ │
         │         │         │  │        │        │
         │  ┌──────▼──────┐  │  │  ┌─────▼──────┐ │
         │  │FileManager  │◄─┼──┼─►│FileManager │ │
         │  │(Replicated) │  │  │  │(Replicated)│ │
         │  │CBOR Serializ│  │  │  │CBOR Serializ│
         │  └──────┬──────┘  │  │  └─────┬──────┘ │
         │         │         │  │        │        │
         │  ┌──────▼──────┐  │  │  ┌─────▼──────┐ │
         │  │PostgreSQL-1 │  │  │  │PostgreSQL-2│ │
         │  └─────────────┘  │  │  └────────────┘ │
         │         │         │  │        │        │
         │  ┌──────▼──────┐  │  │  ┌─────▼──────┐ │
         │  │  MinIO DC1  │  │  │  │  MinIO DC2 │ │
         │  └─────────────┘  │  │  └────────────┘ │
         └───────────────────┘  └─────────────────┘
                   ▲                    ▲
                   └────────────────────┘
                    Event Replication
```

**Architecture Layers:**
1. **Client Layer**: External HTTP/REST clients
2. **REST API Layer**: Public-facing API (Pekko HTTP) - handles HTTP requests, multipart uploads
3. **gRPC Service Layer**: Internal service exposing event sourcing operations (Pekko gRPC)
4. **Event Sourcing Layer**: FileManager actors with CBOR serialization
5. **Persistence Layer**: PostgreSQL with CBOR-serialized events
6. **Storage Layer**: MinIO for file content

**Benefits of this Architecture:**
- **Separation of Concerns**: REST API handles HTTP/multipart, gRPC handles business logic
- **Performance**: CBOR binary serialization is more compact and faster than JSON/XML
- **Type Safety**: Protobuf provides strong typing for gRPC contracts
- **Efficiency**: gRPC uses HTTP/2 for multiplexing and streaming
- **Testability**: Can test gRPC service independently from REST layer
- **Flexibility**: Can add other API layers (GraphQL, WebSocket) calling same gRPC service
- **Internal vs External**: gRPC for internal DC communication, REST for public API

### 2.3 Replicated Event Sourcing Design

#### 2.3.1 Replication Strategy
- **Multi-Master**: All DCs accept writes
- **Eventual Consistency**: Events replicated asynchronously
- **Conflict Resolution**: Last-write-wins with replica ID ordering
- **Replica IDs**: "us-east-1", "eu-west-1", "ap-south-1" (AWS region nomenclature)

#### 2.3.2 Event Journal Configuration
- Separate journal tables per DC
- Replication via Pekko's built-in replication transport
- PostgreSQL as persistence backend (pekko-persistence-jdbc)

---

## 3. Implementation Phases

### Phase 1: Project Setup & Foundation (Week 1)

#### 3.1.1 Project Initialization
- [ ] Create sbt project structure
- [ ] Configure Scala 3.x and Pekko dependencies
- [ ] Set up build.sbt with multi-module structure:
  - `core` - Domain models and business logic
  - `persistence` - Event sourcing, database, and CBOR serialization
  - `protocol` - Protobuf definitions for gRPC
  - `grpc` - gRPC service implementation (internal API)
  - `api` - REST API layer (public API, calls gRPC)
  - `storage` - MinIO/S3 integration
  - `ui` - React web interface (optional)
  - `integration` - End-to-end tests

#### 3.1.2 Dependencies Setup
```scala
// Key dependencies to include:

// Pekko Core
- "org.apache.pekko" %% "pekko-actor-typed"
- "org.apache.pekko" %% "pekko-cluster-typed"
- "org.apache.pekko" %% "pekko-cluster-sharding-typed"
- "org.apache.pekko" %% "pekko-stream"

// Pekko Persistence with CBOR
- "org.apache.pekko" %% "pekko-persistence-typed"
- "org.apache.pekko" %% "pekko-persistence-jdbc"
- "org.apache.pekko" %% "pekko-persistence-query"
- "org.apache.pekko" %% "pekko-serialization-jackson"  // For CBOR serialization
- "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor"

// Pekko gRPC
- "org.apache.pekko" %% "pekko-grpc-runtime"
- "org.apache.pekko" %% "pekko-discovery"

// Pekko HTTP (REST API layer)
- "org.apache.pekko" %% "pekko-http"
- "org.apache.pekko" %% "pekko-http-spray-json"  // or circe for JSON

// Database
- "org.postgresql" % "postgresql"
- "com.zaxxer" % "HikariCP"  // Connection pooling

// MinIO/S3
- "software.amazon.awssdk" % "s3" 
- "software.amazon.awssdk" % "netty-nio-client"

// Configuration
- "com.typesafe" % "config"

// Testing
- "org.apache.pekko" %% "pekko-actor-testkit-typed"
- "org.apache.pekko" %% "pekko-persistence-testkit"
- "org.apache.pekko" %% "pekko-stream-testkit"
- "org.apache.pekko" %% "pekko-http-testkit"
- "org.scalatest" %% "scalatest"
```

**sbt Plugins:**
```scala
// project/plugins.sbt
addSbtPlugin("org.apache.pekko" % "pekko-grpc-sbt-plugin" % "1.0.x")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
```

#### 3.1.3 Configuration Files
- [ ] application.conf (base configuration)
- [ ] dc1.conf (datacenter 1 specific)
- [ ] dc2.conf (datacenter 2 specific)
- [ ] dc3.conf (datacenter 3 specific)
- [ ] docker-compose.yml (local multi-DC simulation with MinIO and PostgreSQL)

#### 3.1.4 Local Development Environment Setup
- [ ] Docker Compose configuration for:
  - 3 PostgreSQL instances (one per datacenter)
  - 3 MinIO instances (S3-compatible storage per datacenter)
  - MinIO console for visual inspection
  - Network configuration for inter-service communication
- [ ] Scripts for initializing buckets and databases
- [ ] Health check endpoints for all services

---

### Phase 2: Domain Model & Event Sourcing (Week 1-2)

#### 3.2.1 Domain Model Implementation
```scala
// File: core/src/main/scala/com/example/filemanager/domain/FileManager.scala

sealed trait Command
sealed trait Event
case class State(...)

object FileManager extends EventSourcedBehavior[Command, Event, State]
```

**Commands to implement:**
- `UploadFile`: Initiate file upload with metadata
- `CompleteUpload`: Confirm S3 upload completion
- `DownloadFile`: Request file download URL
- `DeleteFile`: Mark file as deleted
- `GetFileInfo`: Query current file state

**Events to implement:**
- `FileUploadInitiated`: File upload started
- `FileUploaded`: File successfully uploaded to S3
- `FileDeleted`: File marked as deleted
- `MetadataUpdated`: File metadata changed
- `UploadFailed`: File upload failed

**State management:**
- Maintain current file metadata
- Track upload status
- Handle deletion status

#### 3.2.2 Replicated Event Sourcing Setup
- [ ] Configure ReplicatedEventSourcing behavior
- [ ] Implement replica-aware event handlers
- [ ] Set up conflict resolution strategy
- [ ] Configure replication endpoint discovery

```scala
ReplicatedEventSourcing.commonJournalConfig(
  ReplicationId("file-manager"),
  AllReplicas,
  replicaId => EventSourcedBehavior[Command, Event, State](...)
)
```

#### 3.2.3 CBOR Serialization
- [ ] Configure Jackson CBOR serialization for events and state
- [ ] Define serialization bindings in application.conf
- [ ] Implement CborSerializable marker trait for domain objects
- [ ] Configure serialization manifests for versioning

**CBOR Configuration:**
```hocon
pekko {
  actor {
    serializers {
      jackson-cbor = "org.apache.pekko.serialization.jackson.JacksonCborSerializer"
    }
    
    serialization-bindings {
      "com.jmarin.filemanager.CborSerializable" = jackson-cbor
    }
  }
  
  serialization.jackson {
    # Configuration for jackson CBOR serializer
    jackson-cbor {
      compression {
        algorithm = off
        compress-larger-than = 0 b
      }
    }
  }
}
```

**Domain Objects with CBOR:**
```scala
// Marker trait
trait CborSerializable

// Events with CBOR serialization
sealed trait Event extends CborSerializable
case class FileUploaded(...) extends Event
case class FileDeleted(...) extends Event

// Commands
sealed trait Command extends CborSerializable
case class UploadFile(...) extends Command

// State
case class State(...) extends CborSerializable
```

---

### Phase 3: PostgreSQL Persistence (Week 2)

#### 3.3.1 Database Schema
```sql
-- Event Journal
CREATE TABLE event_journal (
    ordering BIGSERIAL PRIMARY KEY,
    persistence_id VARCHAR(255) NOT NULL,
    sequence_number BIGINT NOT NULL,
    deleted BOOLEAN DEFAULT FALSE,
    tags VARCHAR(255),
    message BYTEA NOT NULL,
    replica_id VARCHAR(255) NOT NULL,
    timestamp BIGINT NOT NULL,
    CONSTRAINT pk_event_journal UNIQUE(persistence_id, sequence_number)
);

-- Snapshot Store
CREATE TABLE snapshot (
    persistence_id VARCHAR(255) NOT NULL,
    sequence_number BIGINT NOT NULL,
    created BIGINT NOT NULL,
    snapshot BYTEA NOT NULL,
    PRIMARY KEY(persistence_id, sequence_number)
);

-- Replication Metadata
CREATE TABLE replication_metadata (
    replica_id VARCHAR(255) NOT NULL,
    persistence_id VARCHAR(255) NOT NULL,
    sequence_number BIGINT NOT NULL,
    PRIMARY KEY(replica_id, persistence_id)
);

-- Read-side: File Metadata Projection
CREATE TABLE file_metadata (
    file_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(500) NOT NULL,
    size_bytes BIGINT NOT NULL,
    mime_type VARCHAR(255) NOT NULL,
    s3_key VARCHAR(1000) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL,
    replica_id VARCHAR(255) NOT NULL
);

CREATE INDEX idx_file_owner ON file_metadata(owner);
CREATE INDEX idx_file_status ON file_metadata(status);
CREATE INDEX idx_file_created ON file_metadata(created_at);
```

#### 3.3.2 JDBC Configuration
- [ ] Configure HikariCP connection pool
- [ ] Set up pekko-persistence-jdbc plugin
- [ ] Configure journal and snapshot tables
- [ ] Set up read-side projections

#### 3.3.3 Read-Side Projections
- [ ] Implement EventHandler for file_metadata table
- [ ] Set up Pekko Projections for query optimization
- [ ] Create indexes for common queries

---

### Phase 4: MinIO Storage Integration (Week 2-3)

#### 3.4.1 S3-Compatible Storage Service Interface
```scala
trait S3StorageService {
  def uploadFile(fileId: String, content: Source[ByteString, Any], 
                 contentLength: Long, mimeType: String): Future[S3Key]
  def downloadFile(s3Key: String): Future[Source[ByteString, Any]]
  def deleteFile(s3Key: String): Future[Unit]
  def generatePresignedUrl(s3Key: String, expiration: Duration): Future[URL]
}
```

#### 3.4.2 MinIO Implementation Details
- [ ] Implement MinIO client using AWS SDK S3 API (S3-compatible)
- [ ] Configure endpoint URLs for each DC MinIO instance
- [ ] Configure bucket per datacenter: `filemanager-dc1`, `filemanager-dc2`, `filemanager-dc3`
- [ ] Handle multipart uploads for large files
- [ ] Implement retry logic and error handling
- [ ] Add presigned URL generation for downloads

#### 3.4.3 MinIO Configuration
**Local Development Setup:**
```hocon
s3 {
  # MinIO endpoint configuration per region (AWS nomenclature)
  us-east-1 {
    endpoint = "http://localhost:9000"
    endpoint = ${?MINIO_US_EAST_1_ENDPOINT}
    bucket = "filemanager-us-east-1"
    access-key = "minioadmin"
    access-key = ${?MINIO_ACCESS_KEY}
    secret-key = "minioadmin123"
    secret-key = ${?MINIO_SECRET_KEY}
    region = "us-east-1"
  }
  
  eu-west-1 {
    endpoint = "http://localhost:9001"
    endpoint = ${?MINIO_EU_WEST_1_ENDPOINT}
    bucket = "filemanager-eu-west-1"
    access-key = "minioadmin"
    secret-key = "minioadmin123"
    region = "eu-west-1"
  }
  
  ap-south-1 {
    endpoint = "http://localhost:9002"
    endpoint = ${?MINIO_AP_SOUTH_1_ENDPOINT}
    bucket = "filemanager-ap-south-1"
    access-key = "minioadmin"
    secret-key = "minioadmin123"
    region = "ap-south-1"
  }
}
```

**Why MinIO for Local Development:**
- S3-compatible API (works with AWS SDK)
- Lightweight and fast
- Easy to run in Docker
- Web console for visual inspection
- No AWS account or costs required
- Perfect for local demonstration

**Alternative: LocalStack**
If preferred, LocalStack can be used instead:
```yaml
localstack:
  image: localstack/localstack:latest
  environment:
    SERVICES: s3
    AWS_DEFAULT_REGION: us-east-1
  ports:
    - "4566:4566"
```

---

### Phase 5: gRPC Service Layer (Week 3)

#### 3.5.1 Protocol Buffer Definitions
Define gRPC service contract in `protocol/src/main/protobuf/filemanager.proto`:

```protobuf
syntax = "proto3";

package com.jmarin.filemanager.grpc;

// File Manager Service
service FileManagerService {
  rpc UploadFile(UploadFileRequest) returns (UploadFileResponse);
  rpc GetFileInfo(GetFileInfoRequest) returns (GetFileInfoResponse);
  rpc DeleteFile(DeleteFileRequest) returns (DeleteFileResponse);
  rpc ListFiles(ListFilesRequest) returns (ListFilesResponse);
  rpc DownloadFile(DownloadFileRequest) returns (DownloadFileResponse);
}

message UploadFileRequest {
  string file_id = 1;
  string name = 2;
  string s3_key = 3;
  int64 size_bytes = 4;
  string mime_type = 5;
  string owner = 6;
  repeated string allowed_regions = 7;  // Empty = unrestricted, otherwise specific regions
  string region = 8;  // The datacenter receiving this upload
}

message UploadFileResponse {
  string file_id = 1;
  FileMetadata metadata = 2;
  ResponseStatus status = 3;
  string error_message = 4;  // Populated on error
}

enum ResponseStatus {
  SUCCESS = 0;
  REGION_RESTRICTION_VIOLATED = 1;
  INVALID_REQUEST = 2;
  INTERNAL_ERROR = 3;
  NOT_FOUND = 4;
  DOWNLOAD_NOT_AUTHORIZED = 5;
}

message GetFileInfoRequest {
  string file_id = 1;
}

message GetFileInfoResponse {
  FileMetadata metadata = 1;
  bool found = 2;
}

message DeleteFileRequest {
  string file_id = 1;
}

message DeleteFileResponse {
  bool success = 1;
}

message ListFilesRequest {
  string owner = 1;
  int32 page = 2;
  int32 page_size = 3;
}

message ListFilesResponse {
  repeated FileMetadata files = 1;
  int32 total_count = 2;
}

message DownloadFileRequest {
  string file_id = 1;
  string region = 2;  // The datacenter handling this download request
}

message DownloadFileResponse {
  string presigned_url = 1;
  string s3_key = 2;
  ResponseStatus status = 3;
  string error_message = 4;  // e.g., "Download not allowed from region 'us-east-1'. Allowed regions: ['eu-west-1', 'eu-central-1']"
}

message FileMetadata {
  string file_id = 1;
  string name = 2;
  int64 size_bytes = 3;
  string mime_type = 4;
  string s3_key = 5;
  string owner = 6;
  int64 created_at = 7;
  int64 modified_at = 8;
  string status = 9;
  repeated string allowed_regions = 10;  // Empty = unrestricted
  string uploaded_region = 11;  // The region where file was originally uploaded
}
```

#### 3.5.2 gRPC Service Implementation
- [ ] Implement FileManagerServiceImpl extending generated trait
- [ ] Integrate with FileManager event sourced actor
- [ ] Handle commands and convert to Protobuf responses
- [ ] Implement regional restriction validation logic
- [ ] Implement error handling with descriptive messages
- [ ] Add request validation
- [ ] Configure gRPC server settings
- [ ] Add region context (from configuration or environment)

**Example Implementation:**
```scala
class FileManagerServiceImpl(
  system: ActorSystem[_],
  sharding: ClusterSharding,
  currentRegion: String  // e.g., "us-east-1", "eu-west-1", "ap-south-1"
) extends FileManagerService {
  
  override def uploadFile(request: UploadFileRequest): Future[UploadFileResponse] = {
    // Validate region restrictions
    if (request.allowedRegions.nonEmpty && !request.allowedRegions.contains(currentRegion)) {
      return Future.successful(
        UploadFileResponse(
          fileId = request.fileId,
          metadata = None,
          status = ResponseStatus.REGION_RESTRICTION_VIOLATED,
          errorMessage = s"Upload not allowed in region '$currentRegion'. " +
            s"Allowed regions: ${request.allowedRegions.mkString("[", ", ", "]")}"
        )
      )
    }
    
    val entityRef = sharding.entityRefFor(FileManager.EntityKey, request.fileId)
    
    entityRef.ask(replyTo => 
      FileManager.CompleteUpload(
        request.fileId,
        request.name,
        request.s3Key,
        request.sizeBytes,
        request.mimeType,
        request.owner,
        request.allowedRegions.toSet,
        currentRegion,
        replyTo
      )
    ).map {
      case FileManager.UploadCompleted(metadata) =>
        UploadFileResponse(
          request.fileId, 
          Some(toProto(metadata)), 
          ResponseStatus.SUCCESS,
          ""
        )
      case FileManager.UploadFailed(reason) =>
        UploadFileResponse(
          request.fileId, 
          None, 
          ResponseStatus.INTERNAL_ERROR,
          s"Upload failed: $reason"
        )
    }
  }
  
  override def downloadFile(request: DownloadFileRequest): Future[DownloadFileResponse] = {
    val entityRef = sharding.entityRefFor(FileManager.EntityKey, request.fileId)
    
    entityRef.ask(replyTo => 
      FileManager.RequestDownload(request.fileId, currentRegion, replyTo)
    ).map {
      case FileManager.DownloadAuthorized(s3Key, presignedUrl) =>
        DownloadFileResponse(
          presignedUrl = presignedUrl,
          s3Key = s3Key,
          status = ResponseStatus.SUCCESS,
          errorMessage = ""
        )
      case FileManager.DownloadDenied(allowedRegions) =>
        DownloadFileResponse(
          presignedUrl = "",
          s3Key = "",
          status = ResponseStatus.DOWNLOAD_NOT_AUTHORIZED,
          errorMessage = if (allowedRegions.isEmpty) 
            s"File not found or has been deleted"
          else
            s"Download not allowed from region '$currentRegion'. " +
            s"Allowed regions: ${allowedRegions.mkString("[", ", ", "]")}"
        )
    }
  }
  
  // ... other methods
}
```

#### 3.5.3 gRPC Server Configuration
```hocon
pekko.grpc.server {
  host = "0.0.0.0"
  port = 9090
  port = ${?GRPC_PORT}
  
  # Use plaintext for local development
  use-tls = false
}
```

---

### Phase 6: REST API Layer (Week 3-4)

#### 3.6.1 REST Endpoints (Public API)
```
POST   /api/files              - Upload file (multipart)
                                  Query params: ?allowedRegions=eu-west-1,eu-central-1 (optional)
GET    /api/files/:id          - Get file metadata
GET    /api/files/:id/download - Get download URL (validates region)
DELETE /api/files/:id          - Delete file
GET    /api/files              - List files (with pagination)
```

**Regional Error Responses:**
```json
// 403 Forbidden - Upload to wrong region
{
  "error": "REGION_RESTRICTION_VIOLATED",
  "message": "Upload not allowed in region 'us-east-1'. Allowed regions: ['eu-west-1', 'eu-central-1']",
  "fileId": "abc-123",
  "currentRegion": "us-east-1"
}

// 403 Forbidden - Download from wrong region
{
  "error": "DOWNLOAD_NOT_AUTHORIZED",
  "message": "Download not allowed from region 'us-east-1'. Allowed regions: ['eu-west-1', 'eu-central-1']",
  "fileId": "abc-123",
  "currentRegion": "us-east-1"
}
```

#### 3.6.2 File Upload Flow (REST → gRPC) with Regional Validation
1. Client sends multipart HTTP upload to REST API with optional `allowedRegions` param
2. REST handler streams file to MinIO
3. MinIO returns S3 key
4. REST API calls gRPC service with file metadata + allowed regions
5. gRPC service validates current region against allowed regions
   - If restricted and region not allowed → return REGION_RESTRICTION_VIOLATED
   - If allowed or unrestricted → proceed
6. gRPC service sends command to FileManager actor
7. FileManager persists `FileUploaded` event (CBOR serialized) with region info
8. Event replicates to other DCs (metadata only, file stays in original MinIO)
9. gRPC response sent back to REST API
10. REST API translates gRPC status to HTTP status code and returns JSON response

**Download Flow with Regional Validation:**
1. Client requests download from specific DC
2. REST API calls gRPC service with fileId + current region
3. FileManager actor checks if file's allowed regions include current region
   - If allowed → generate presigned URL and return
   - If denied → return DOWNLOAD_NOT_AUTHORIZED with descriptive message
4. REST API returns appropriate HTTP status (200 OK or 403 Forbidden)

**Flow Diagram:**
```
Client → REST API → MinIO (upload)
              ↓
         gRPC Service → FileManager Actor → PostgreSQL (CBOR events)
              ↓
         REST API ← gRPC Response
              ↓
         Client ← JSON Response
```

#### 3.6.3 REST API Implementation
- [ ] Create Pekko HTTP routes
- [ ] Implement multipart form data handling for file uploads
- [ ] Parse `allowedRegions` query parameter
- [ ] Create gRPC client to call FileManagerService
- [ ] Implement request/response DTOs (JSON)
- [ ] Map gRPC ResponseStatus to HTTP status codes:
  - SUCCESS → 200 OK / 201 Created
  - REGION_RESTRICTION_VIOLATED → 403 Forbidden
  - DOWNLOAD_NOT_AUTHORIZED → 403 Forbidden
  - NOT_FOUND → 404 Not Found
  - INVALID_REQUEST → 400 Bad Request
  - INTERNAL_ERROR → 500 Internal Server Error
- [ ] Add descriptive error handling with regional context
- [ ] Add request validation
- [ ] Configure CORS if needed
- [ ] Implement health check endpoint

**Example REST Route with Regional Validation:**
```scala
class FileManagerRoutes(
  grpcClient: FileManagerServiceClient,
  s3Service: S3StorageService,
  currentRegion: String  // Injected from configuration
)(implicit ec: ExecutionContext) {
  
  val routes: Route = pathPrefix("api" / "files") {
    concat(
      // Upload file with optional regional restrictions
      post {
        fileUpload("file") { case (metadata, byteSource) =>
          parameters("owner", "allowedRegions".optional) { (owner, allowedRegionsParam) =>
            val allowedRegions = allowedRegionsParam
              .map(_.split(",").map(_.trim).toSeq)
              .getOrElse(Seq.empty)
            
            onSuccess(uploadFile(metadata, byteSource, owner, allowedRegions)) {
              case response if response.status == ResponseStatus.SUCCESS =>
                complete(StatusCodes.Created, response.toJson)
              case response if response.status == ResponseStatus.REGION_RESTRICTION_VIOLATED =>
                complete(StatusCodes.Forbidden, ErrorResponse(
                  error = "REGION_RESTRICTION_VIOLATED",
                  message = response.errorMessage,
                  fileId = response.fileId,
                  currentRegion = currentRegion
                ))
              case response =>
                complete(StatusCodes.InternalServerError, ErrorResponse(
                  error = "INTERNAL_ERROR",
                  message = response.errorMessage,
                  fileId = response.fileId,
                  currentRegion = currentRegion
                ))
            }
          }
        }
      },
      // Download file with regional validation
      path(Segment / "download") { fileId =>
        get {
          onSuccess(grpcClient.downloadFile(
            DownloadFileRequest(fileId = fileId, region = currentRegion)
          )) {
            case response if response.status == ResponseStatus.SUCCESS =>
              complete(StatusCodes.OK, DownloadResponse(
                presignedUrl = response.presignedUrl,
                s3Key = response.s3Key
              ))
            case response if response.status == ResponseStatus.DOWNLOAD_NOT_AUTHORIZED =>
              complete(StatusCodes.Forbidden, ErrorResponse(
                error = "DOWNLOAD_NOT_AUTHORIZED",
                message = response.errorMessage,
                fileId = fileId,
                currentRegion = currentRegion
              ))
            case response if response.status == ResponseStatus.NOT_FOUND =>
              complete(StatusCodes.NotFound, ErrorResponse(
                error = "NOT_FOUND",
                message = "File not found",
                fileId = fileId,
                currentRegion = currentRegion
              ))
            case response =>
              complete(StatusCodes.InternalServerError, ErrorResponse(
                error = "INTERNAL_ERROR",
                message = response.errorMessage,
                fileId = fileId,
                currentRegion = currentRegion
              ))
          }
        }
      },
      // Get file info
      path(Segment) { fileId =>
        get {
          onSuccess(grpcClient.getFileInfo(GetFileInfoRequest(fileId))) {
            case response if response.found =>
              complete(StatusCodes.OK, toJson(response.metadata))
            case _ =>
              complete(StatusCodes.NotFound, ErrorResponse(
                error = "NOT_FOUND",
                message = "File not found",
                fileId = fileId,
                currentRegion = currentRegion
              ))
          }
        }
      }
    )
  }
  
  private def uploadFile(
    metadata: FileInfo,
    byteSource: Source[ByteString, Any],
    owner: String,
    allowedRegions: Seq[String]
  ): Future[UploadFileResponse] = {
    for {
      // Upload to MinIO
      s3Key <- s3Service.uploadFile(
        UUID.randomUUID().toString,
        byteSource,
        metadata.contentLength,
        metadata.contentType.toString()
      )
      // Call gRPC service with regional restrictions
      grpcResponse <- grpcClient.uploadFile(
        UploadFileRequest(
          fileId = s3Key,
          name = metadata.fileName,
          s3Key = s3Key,
          sizeBytes = metadata.contentLength,
          mimeType = metadata.contentType.toString(),
          owner = owner,
          allowedRegions = allowedRegions,
          region = currentRegion
        )
      )
    } yield grpcResponse
  }
}

// JSON response models
case class ErrorResponse(
  error: String,
  message: String,
  fileId: String,
  currentRegion: String
)

case class DownloadResponse(
  presignedUrl: String,
  s3Key: String
)
```

---

### Phase 7: Multi-DC Configuration (Week 4)

#### 3.7.1 Cluster Configuration
```hocon
pekko {
  actor {
    provider = cluster
    
    serialization-bindings {
      # CBOR serialization for events, commands, and state
      "com.jmarin.filemanager.CborSerializable" = jackson-cbor
    }
  }
  
  cluster {
    seed-nodes = [
      "pekko://FileManager@us-east-1-node1:2551",
      "pekko://FileManager@eu-west-1-node1:2551"
    ]
    
    multi-data-center {
      self-data-center = "us-east-1"
    }
  }
  
  persistence {
    journal.plugin = "jdbc-journal"
    snapshot-store.plugin = "jdbc-snapshot-store"
    
    state {
      plugin = "jdbc-durable-state-store"
    }
  }
}

jdbc-journal {
  slick = ${slick}
}

jdbc-snapshot-store {
  slick = ${slick}
}

slick {
  profile = "slick.jdbc.PostgresProfile$"
  db {
    host = "localhost"
    host = ${?POSTGRES_HOST}
    port = 5432
    port = ${?POSTGRES_PORT}
    name = "filemanager"
    name = ${?POSTGRES_DB}
    user = "filemanager_user"
    user = ${?POSTGRES_USER}
    password = "secure_password"
    password = ${?POSTGRES_PASSWORD}
    driver = "org.postgresql.Driver"
    numThreads = 20
    maxConnections = 20
    minConnections = 5
  }
}
```

#### 3.7.2 Replication Configuration
```hocon
pekko.persistence.typed.replicated-event-sourcing {
  # Replica IDs for multi-DC setup using AWS region nomenclature
  replicas = ["us-east-1", "eu-west-1", "ap-south-1"]
  
  # Fast lane for replication
  replication-transport-settings {
    parallel-updates = 8
  }
}
```

#### 3.7.3 Port Allocation
**Per Datacenter:**
- **us-east-1**: REST API :8081, gRPC :9090, Pekko :2551, PostgreSQL :5432, MinIO :9000, MinIO Console :9100
- **eu-west-1**: REST API :8082, gRPC :9091, Pekko :2552, PostgreSQL :5433, MinIO :9001, MinIO Console :9101
- **ap-south-1**: REST API :8083, gRPC :9092, Pekko :2553, PostgreSQL :5434, MinIO :9002, MinIO Console :9102

#### 3.7.4 Docker Compose for Local Demonstration
Complete local environment simulating 3 datacenters:

**Infrastructure per DC:**
- [ ] PostgreSQL instance (ports: 5432, 5433, 5434)
- [ ] MinIO instance (S3-compatible storage, ports: 9000-9002 for API, 9100-9102 for console)
- [ ] Pekko application instance (ports: 8081-8083 for REST API, 9090-9092 for gRPC, 2551-2553 for clustering)

**Shared Services:**
- [ ] MinIO console for visual bucket/object inspection
- [ ] pgAdmin (optional) for database inspection

**Docker Compose Configuration:**
```yaml
version: '3.8'
services:
  # us-east-1
  postgres-us-east-1:
    image: postgres:15
    environment:
      POSTGRES_DB: filemanager_us_east_1
      POSTGRES_USER: filemanager
      POSTGRES_PASSWORD: filemanager123
    ports:
      - "5432:5432"
    volumes:
      - postgres-us-east-1-data:/var/lib/postgresql/data
  
  minio-us-east-1:
    image: minio/minio:latest
    command: server /data --console-address ":9090"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin123
    ports:
      - "9000:9000"
      - "9100:9090"  # Console on 9100 to avoid conflict with gRPC
    volumes:
      - minio-us-east-1-data:/data
  
  # eu-west-1
  postgres-eu-west-1:
    image: postgres:15
    environment:
      POSTGRES_DB: filemanager_eu_west_1
      POSTGRES_USER: filemanager
      POSTGRES_PASSWORD: filemanager123
    ports:
      - "5433:5432"
    volumes:
      - postgres-eu-west-1-data:/var/lib/postgresql/data
  
  minio-eu-west-1:
    image: minio/minio:latest
    command: server /data --console-address ":9090"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin123
    ports:
      - "9001:9000"
      - "9101:9090"  # Console on 9101
    volumes:
      - minio-eu-west-1-data:/data
  
  # ap-south-1
  postgres-ap-south-1:
    image: postgres:15
    environment:
      POSTGRES_DB: filemanager_ap_south_1
      POSTGRES_USER: filemanager
      POSTGRES_PASSWORD: filemanager123
    ports:
      - "5434:5432"
    volumes:
      - postgres-ap-south-1-data:/var/lib/postgresql/data
  
  minio-ap-south-1:
    image: minio/minio:latest
    command: server /data --console-address ":9090"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin123
    ports:
      - "9002:9000"
      - "9102:9090"  # Console on 9102
    volumes:
      - minio-ap-south-1-data:/data

volumes:
  postgres-us-east-1-data:
  postgres-eu-west-1-data:
  postgres-ap-south-1-data:
  minio-us-east-1-data:
  minio-eu-west-1-data:
  minio-ap-south-1-data:
```

**Initialization Scripts:**
- [ ] Create buckets in each MinIO instance (`filemanager-us-east-1`, `filemanager-eu-west-1`, `filemanager-ap-south-1`)
- [ ] Initialize PostgreSQL schemas
- [ ] Verify connectivity between services

---

### Phase 8: Testing Strategy (Week 4-5)

#### 3.8.1 Unit Tests
- [ ] Domain model command handlers
- [ ] Event application logic
- [ ] State transition tests
- [ ] CBOR serialization/deserialization tests
- [ ] gRPC service unit tests (with mocks)

#### 3.8.2 Integration Tests
- [ ] Event persistence to PostgreSQL with CBOR
- [ ] Event replication between replicas
- [ ] MinIO upload/download operations
- [ ] gRPC service integration tests
- [ ] REST API endpoint tests (calling gRPC)
- [ ] End-to-end flow: REST → gRPC → EventSourcing → PostgreSQL
- [ ] Full Docker Compose environment tests

#### 3.8.3 Multi-DC Scenario Tests (Local Environment)
- [ ] Concurrent writes to different DCs (localhost:8081, 8082, 8083)
- [ ] Network partition simulation using Docker networking
- [ ] Eventual consistency verification across 3 local nodes
- [ ] Conflict resolution validation
- [ ] File upload to DC1, read from DC2/DC3 (after replication)
- [ ] Simulate DC failure by stopping container

#### 3.8.4 gRPC Testing
- [ ] Test gRPC service directly using grpcurl
- [ ] Test REST-to-gRPC communication
- [ ] Verify Protobuf serialization
- [ ] Test gRPC error handling

**Testing gRPC Service:**
```bash
# Install grpcurl
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# List services
grpcurl -plaintext localhost:9090 list

# Call UploadFile
grpcurl -plaintext -d '{
  "file_id": "test-123",
  "name": "test.pdf",
  "s3_key": "files/test-123",
  "size_bytes": 1024,
  "mime_type": "application/pdf",
  "owner": "user@example.com"
}' localhost:9090 com.jmarin.filemanager.grpc.FileManagerService/UploadFile

# Get file info
grpcurl -plaintext -d '{"file_id": "test-123"}' \
  localhost:9090 com.jmarin.filemanager.grpc.FileManagerService/GetFileInfo
```

#### 3.8.5 Local Demonstration Scenarios
**Scenario 1: Basic Multi-Region Upload**
1. Upload file to DC1 (localhost:8081)
2. Verify file appears in MinIO DC1 bucket
3. Query file metadata from DC2 (localhost:8082)
4. Verify eventual consistency

**Scenario 2: Concurrent Uploads**
1. Upload file A to DC1
2. Upload file B to DC2 simultaneously
3. Upload file C to DC3 simultaneously
4. Verify all files replicated to all DCs

**Scenario 3: Conflict Resolution**
1. Update file metadata in DC1
2. Update same file metadata in DC2 concurrently
3. Verify conflict resolution using replica ID ordering

**Scenario 4: DC Failure Recovery**
1. Stop DC2 container
2. Upload files to DC1 and DC3
3. Restart DC2
4. Verify DC2 catches up with missed events

**Scenario 5: Regional Restrictions - EU-Only Upload**
1. Upload file to DC2 (EU-West) with `allowedRegions=eu-west-1`
2. Verify upload succeeds (201 Created)
3. Try to upload same file to DC1 (US-East) with same restrictions
4. Verify upload fails with 403 Forbidden
5. Expected error: "Upload not allowed in region 'us-east-1'. Allowed regions: ['eu-west-1']"

**Scenario 6: Regional Restrictions - Download Enforcement**
1. Upload file to DC2 with `allowedRegions=eu-west-1,eu-central-1`
2. Attempt download from DC2 (EU-West)
3. Verify download succeeds (200 OK with presigned URL)
4. Attempt download from DC1 (US-East)
5. Verify download fails with 403 Forbidden
6. Expected error: "Download not allowed from region 'us-east-1'. Allowed regions: ['eu-west-1', 'eu-central-1']"

**Scenario 7: Unrestricted File**
1. Upload file to DC1 without `allowedRegions` parameter (default)
2. Verify upload succeeds
3. Download from DC1, DC2, and DC3
4. Verify all downloads succeed

**Scenario 8: Multi-Region Allowed File**
1. Upload file with `allowedRegions=us-east-1,ap-south-1`
2. Upload to DC1 (US-East) - should succeed
3. Upload to DC3 (AP-South) - should succeed
4. Upload to DC2 (EU-West) - should fail with 403
5. Download from DC1 - should succeed
6. Download from DC2 - should fail with 403
7. Download from DC3 - should succeed

#### 3.8.6 Testing Tools
```scala
// Use Pekko TestKit
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit

class FileManagerSpec extends ScalaTestWithActorTestKit 
  with AnyWordSpecLike {
  
  val testKit = EventSourcedBehaviorTestKit[Command, Event, State](
    system, FileManager("test-file-1", "us-east-1")
  )
  
  "FileManager" should {
    "handle file upload" in {
      val result = testKit.runCommand(UploadFile(...))
      result.event shouldBe FileUploaded(...)
    }
  }
}
```

---

### Phase 9: Monitoring & Observability (Week 5)

#### 3.9.1 Metrics
- [ ] Event processing rate per DC
- [ ] Replication lag between DCs
- [ ] Command processing latency
- [ ] gRPC service metrics (request rate, latency, errors)
- [ ] REST API metrics
- [ ] MinIO operation metrics
- [ ] Database connection pool metrics
- [ ] CBOR serialization/deserialization performance

#### 3.9.2 Logging
- [ ] Structured logging with Logback
- [ ] Correlation IDs for request tracking
- [ ] Event sourcing audit trail

#### 3.9.3 Health Checks
- [ ] Cluster membership health
- [ ] Database connectivity (PostgreSQL)
- [ ] MinIO connectivity
- [ ] gRPC service health
- [ ] REST API health endpoint
- [ ] Replication status

---

### Phase 10: React Web UI (Week 5-6) - Optional Module

This is an optional module that provides a user-friendly web interface for the file manager system.

#### 3.10.1 UI Technology Stack
```json
{
  "dependencies": {
    "react": "^18.x",
    "react-dom": "^18.x",
    "axios": "^1.x",
    "react-dropzone": "^14.x",
    "tailwindcss": "^3.x",
    "vite": "^5.x"
  }
}
```

#### 3.10.2 Core Features
- [ ] **Multi-Region Selection**: Dropdown to select target region (us-east-1, eu-west-1, ap-south-1)
- [ ] **Drag & Drop Upload**: React Dropzone component for intuitive file uploads
- [ ] **Multi-File Upload**: Support uploading multiple files simultaneously
- [ ] **Upload Progress**: Real-time progress bars for each file being uploaded
- [ ] **Regional Restrictions UI**: Optional checkboxes to restrict uploads to specific regions
- [ ] **File List Display**: Table/grid showing uploaded files with metadata
- [ ] **File Actions**: Download and delete buttons per file
- [ ] **Error Handling**: Display descriptive errors from REST API (403 regional violations)
- [ ] **Real-time Updates**: Optional polling or WebSocket for file list updates

#### 3.10.3 Component Structure
```
ui/
├── public/
│   └── index.html
├── src/
│   ├── components/
│   │   ├── RegionSelector.tsx        # Dropdown for selecting region
│   │   ├── FileUploadZone.tsx        # Drag & drop upload component
│   │   ├── UploadProgress.tsx        # Progress bar per file
│   │   ├── FileList.tsx              # Display uploaded files
│   │   ├── FileItem.tsx              # Individual file row/card
│   │   ├── RegionRestrictions.tsx    # Regional restrictions checkboxes
│   │   └── ErrorAlert.tsx            # Error message display
│   ├── services/
│   │   └── api.ts                    # REST API client (axios)
│   ├── types/
│   │   └── index.ts                  # TypeScript types
│   ├── App.tsx                       # Main application component
│   ├── main.tsx                      # Entry point
│   └── index.css                     # Tailwind CSS
├── package.json
├── vite.config.ts
└── tsconfig.json
```

#### 3.10.4 Key Components Implementation

**RegionSelector Component:**
```tsx
interface RegionSelectorProps {
  selectedRegion: string;
  onRegionChange: (region: string) => void;
}

const RegionSelector: React.FC<RegionSelectorProps> = ({ 
  selectedRegion, 
  onRegionChange 
}) => {
  const regions = [
    { id: 'us-east-1', name: 'US East (Virginia)', apiUrl: 'http://localhost:8081' },
    { id: 'eu-west-1', name: 'EU West (Ireland)', apiUrl: 'http://localhost:8082' },
    { id: 'ap-south-1', name: 'Asia Pacific (Mumbai)', apiUrl: 'http://localhost:8083' }
  ];

  return (
    <select 
      value={selectedRegion} 
      onChange={(e) => onRegionChange(e.target.value)}
      className="px-4 py-2 border rounded"
    >
      {regions.map(region => (
        <option key={region.id} value={region.id}>
          {region.name}
        </option>
      ))}
    </select>
  );
};
```

**FileUploadZone Component:**
```tsx
import { useDropzone } from 'react-dropzone';

interface FileUploadZoneProps {
  onFilesSelected: (files: File[]) => void;
  selectedRegion: string;
  allowedRegions: string[];
}

const FileUploadZone: React.FC<FileUploadZoneProps> = ({ 
  onFilesSelected, 
  selectedRegion,
  allowedRegions 
}) => {
  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop: (acceptedFiles) => onFilesSelected(acceptedFiles),
    multiple: true
  });

  return (
    <div 
      {...getRootProps()} 
      className={`border-2 border-dashed rounded-lg p-10 text-center cursor-pointer
        ${isDragActive ? 'border-blue-500 bg-blue-50' : 'border-gray-300'}`}
    >
      <input {...getInputProps()} />
      {isDragActive ? (
        <p>Drop files here...</p>
      ) : (
        <div>
          <p className="text-lg">Drag & drop files here, or click to select</p>
          <p className="text-sm text-gray-500 mt-2">
            Uploading to: <strong>{selectedRegion}</strong>
          </p>
          {allowedRegions.length > 0 && (
            <p className="text-sm text-amber-600 mt-1">
              Restricted to: {allowedRegions.join(', ')}
            </p>
          )}
        </div>
      )}
    </div>
  );
};
```

**UploadProgress Component:**
```tsx
interface UploadProgressProps {
  fileName: string;
  progress: number;
  status: 'uploading' | 'success' | 'error';
  errorMessage?: string;
}

const UploadProgress: React.FC<UploadProgressProps> = ({ 
  fileName, 
  progress, 
  status,
  errorMessage 
}) => {
  return (
    <div className="mb-4">
      <div className="flex justify-between mb-1">
        <span className="text-sm font-medium">{fileName}</span>
        <span className="text-sm">
          {status === 'uploading' && `${progress}%`}
          {status === 'success' && '✓ Complete'}
          {status === 'error' && '✗ Failed'}
        </span>
      </div>
      <div className="w-full bg-gray-200 rounded-full h-2">
        <div 
          className={`h-2 rounded-full transition-all ${
            status === 'success' ? 'bg-green-500' : 
            status === 'error' ? 'bg-red-500' : 
            'bg-blue-500'
          }`}
          style={{ width: `${progress}%` }}
        />
      </div>
      {errorMessage && (
        <p className="text-sm text-red-500 mt-1">{errorMessage}</p>
      )}
    </div>
  );
};
```

**FileList Component:**
```tsx
interface FileMetadata {
  fileId: string;
  name: string;
  sizeBytes: number;
  mimeType: string;
  owner: string;
  createdAt: number;
  allowedRegions: string[];
  uploadedRegion: string;
}

const FileList: React.FC<{ 
  files: FileMetadata[], 
  currentRegion: string,
  onDownload: (fileId: string) => void,
  onDelete: (fileId: string) => void
}> = ({ files, currentRegion, onDownload, onDelete }) => {
  return (
    <div className="mt-8">
      <h2 className="text-xl font-bold mb-4">Uploaded Files</h2>
      <table className="min-w-full bg-white border">
        <thead>
          <tr className="bg-gray-100">
            <th className="px-4 py-2 border">Name</th>
            <th className="px-4 py-2 border">Size</th>
            <th className="px-4 py-2 border">Type</th>
            <th className="px-4 py-2 border">Uploaded Region</th>
            <th className="px-4 py-2 border">Restrictions</th>
            <th className="px-4 py-2 border">Actions</th>
          </tr>
        </thead>
        <tbody>
          {files.map(file => (
            <tr key={file.fileId}>
              <td className="px-4 py-2 border">{file.name}</td>
              <td className="px-4 py-2 border">{formatBytes(file.sizeBytes)}</td>
              <td className="px-4 py-2 border">{file.mimeType}</td>
              <td className="px-4 py-2 border">{file.uploadedRegion}</td>
              <td className="px-4 py-2 border">
                {file.allowedRegions.length > 0 
                  ? file.allowedRegions.join(', ') 
                  : 'None'}
              </td>
              <td className="px-4 py-2 border">
                <button 
                  onClick={() => onDownload(file.fileId)}
                  className="px-2 py-1 bg-blue-500 text-white rounded mr-2"
                >
                  Download
                </button>
                <button 
                  onClick={() => onDelete(file.fileId)}
                  className="px-2 py-1 bg-red-500 text-white rounded"
                >
                  Delete
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
```

**API Service:**
```typescript
// src/services/api.ts
import axios, { AxiosProgressEvent } from 'axios';

const getApiUrl = (region: string): string => {
  const urls: Record<string, string> = {
    'us-east-1': 'http://localhost:8081',
    'eu-west-1': 'http://localhost:8082',
    'ap-south-1': 'http://localhost:8083'
  };
  return urls[region];
};

export const uploadFile = async (
  region: string,
  file: File,
  owner: string,
  allowedRegions: string[],
  onProgress: (progress: number) => void
) => {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('owner', owner);

  const url = `${getApiUrl(region)}/api/files${
    allowedRegions.length > 0 ? `?allowedRegions=${allowedRegions.join(',')}` : ''
  }`;

  return axios.post(url, formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
    onUploadProgress: (progressEvent: AxiosProgressEvent) => {
      const progress = progressEvent.total 
        ? Math.round((progressEvent.loaded * 100) / progressEvent.total)
        : 0;
      onProgress(progress);
    }
  });
};

export const listFiles = async (region: string) => {
  const url = `${getApiUrl(region)}/api/files`;
  return axios.get(url);
};

export const downloadFile = async (region: string, fileId: string) => {
  const url = `${getApiUrl(region)}/api/files/${fileId}/download`;
  return axios.get(url);
};

export const deleteFile = async (region: string, fileId: string) => {
  const url = `${getApiUrl(region)}/api/files/${fileId}`;
  return axios.delete(url);
};
```

#### 3.10.5 Setup and Running

**Installation:**
```bash
cd ui
npm install
```

**Development:**
```bash
npm run dev
# Opens at http://localhost:5173
```

**Production Build:**
```bash
npm run build
# Output in ui/dist/
```

**Docker Integration (Optional):**
```yaml
# Add to docker-compose.yml
ui:
  build: ./ui
  ports:
    - "5173:5173"
  environment:
    - VITE_API_US_EAST_1=http://localhost:8081
    - VITE_API_EU_WEST_1=http://localhost:8082
    - VITE_API_AP_SOUTH_1=http://localhost:8083
```

#### 3.10.6 Features Demonstration

**Scenario 1: Basic Upload**
1. User selects region (e.g., us-east-1)
2. Drags files into upload zone
3. Files upload with progress bars
4. Success notification appears
5. Files appear in the list below

**Scenario 2: Regional Restriction Upload**
1. User checks "Restrict to specific regions"
2. Selects eu-west-1 only
3. Tries to upload to us-east-1
4. Error appears: "Upload not allowed in region 'us-east-1'"
5. Switches to eu-west-1
6. Upload succeeds

**Scenario 3: Download with Regional Restrictions**
1. User uploads file restricted to eu-west-1
2. File appears in list with "eu-west-1" in restrictions column
3. User clicks download from us-east-1 region
4. Error appears: "Download not allowed from region 'us-east-1'"
5. User switches to eu-west-1
6. Download succeeds

#### 3.10.7 UI Testing
- [ ] Unit tests with React Testing Library
- [ ] Component tests for upload/download flows
- [ ] E2E tests with Playwright or Cypress
- [ ] Test error handling for regional violations
- [ ] Test multi-file upload scenarios

---

## 4. Deployment Architecture

### 4.1 Local Demonstration Environment (Primary Focus)

**This project is designed to run completely locally for demonstration purposes.**

#### Docker Compose Infrastructure:
- **3 Pekko Application Instances**: One per simulated datacenter
  - us-east-1: localhost:8081 (HTTP), localhost:2551 (Pekko remoting)
  - eu-west-1: localhost:8082 (HTTP), localhost:2552 (Pekko remoting)
  - ap-south-1: localhost:8083 (HTTP), localhost:2553 (Pekko remoting)

- **3 PostgreSQL Instances**: Isolated databases per region
  - us-east-1: localhost:5432
  - eu-west-1: localhost:5433
  - ap-south-1: localhost:5434

- **3 MinIO Instances**: S3-compatible storage per region
  - us-east-1: localhost:9000 (API), localhost:9100 (Console)
  - eu-west-1: localhost:9001 (API), localhost:9101 (Console)
  - ap-south-1: localhost:9002 (API), localhost:9102 (Console)

**Access Points:**
- REST API: http://localhost:8081, http://localhost:8082, http://localhost:8083
- gRPC Services: localhost:9090, localhost:9091, localhost:9092 (use grpcurl)
- MinIO Consoles: http://localhost:9100, http://localhost:9101, http://localhost:9102
- PostgreSQL: Standard psql client to ports 5432-5434

### 4.2 Production Deployment (Reference Only)

#### Per Datacenter:
- **Application Nodes**: 2-3 Pekko instances (for HA)
- **PostgreSQL**: Primary with read replicas
- **S3**: Regional bucket (AWS S3, not MinIO)
- **Load Balancer**: Distribute HTTP requests
- **Network**: Low-latency inter-DC connectivity

### 4.3 Kubernetes Deployment (Optional - Production Reference)
```yaml
# Example StatefulSet for Pekko cluster
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: filemanager-dc1
spec:
  serviceName: filemanager
  replicas: 3
  selector:
    matchLabels:
      app: filemanager
      dc: dc1
  template:
    metadata:
      labels:
        app: filemanager
        dc: dc1
    spec:
      containers:
      - name: filemanager
        image: filemanager:latest
        env:
        - name: DATACENTER_ID
          value: "dc-us-east-1"
        - name: POSTGRES_HOST
          value: "postgres-dc1"
        - name: S3_BUCKET
          value: "filemanager-us-east-1"
```

---

## 5. Key Challenges & Solutions

### 5.1 Challenge: Large File Handling
**Solution**: 
- Stream files directly to S3 without loading into memory
- Use Pekko Streams for backpressure management
- Implement chunked uploads

### 5.2 Challenge: Conflict Resolution
**Solution**:
- Use replica ID ordering for deterministic conflict resolution
- Implement version vectors for causality tracking
- Last-write-wins with timestamp + replica ID

### 5.3 Challenge: Cross-DC Latency
**Solution**:
- Accept writes locally, replicate asynchronously
- Provide eventual consistency guarantees
- Use read-local pattern for queries

### 5.4 Challenge: Data Consistency During Network Partitions
**Solution**:
- Continue accepting writes in each DC
- Reconcile when partition heals
- Use CRDTs principles for conflict-free merges

### 5.5 Challenge: Regional Data Residency Compliance
**Solution**:
- Enforce upload restrictions at gRPC service layer
- Validate download requests against allowed regions
- Store regional metadata in event sourcing state
- Provide descriptive error messages for compliance violations
- File physical storage remains in original region's MinIO
- Metadata replicates globally, but access is region-controlled

---

## 6. Success Criteria

**Local Demonstration Requirements:**
- [ ] Complete system runs locally via `docker-compose up`
- [ ] 3 datacenters (Pekko nodes) running simultaneously on localhost
- [ ] File uploads work from any DC (localhost:8081, 8082, or 8083)
- [ ] Files visible in MinIO console after upload
- [ ] Metadata visible across all DCs within 2-5 seconds (eventual consistency)
- [ ] System handles simulated DC failures (stopping Docker containers)
- [ ] No data loss during DC failures
- [ ] Read operations are strongly consistent within a DC
- [ ] Cross-DC operations are eventually consistent
- [ ] System recovers automatically when DC comes back online
- [ ] Demo script successfully runs all test scenarios

**Regional Restrictions Requirements:**
- [ ] Unrestricted files can be uploaded to any datacenter
- [ ] Region-restricted uploads are enforced (403 Forbidden for wrong regions)
- [ ] Region-restricted downloads are enforced (403 Forbidden for wrong regions)
- [ ] Multi-region allowed files work correctly across specified regions
- [ ] Descriptive error messages in gRPC responses with region info
- [ ] Descriptive error messages in REST API JSON responses with region info
- [ ] Regional metadata persists in CBOR-serialized events
- [ ] Regional metadata replicates across all DCs
- [ ] File physical storage respects original upload region

---

## 7. Project Timeline

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| 1. Project Setup | Week 1 | Working sbt project + Docker Compose + Protocol definitions |
| 2. Domain Model | Week 1-2 | Event sourced FileManager entity with CBOR serialization |
| 3. PostgreSQL | Week 2 | Persistence layer with CBOR events and projections |
| 4. MinIO Integration | Week 2-3 | File upload/download working with MinIO |
| 5. gRPC Service | Week 3 | gRPC service layer exposing event sourcing operations |
| 6. REST API | Week 3-4 | REST endpoints calling gRPC service |
| 7. Multi-DC Setup | Week 4 | 3-DC local environment running on localhost |
| 8. Testing | Week 4-5 | Comprehensive test suite + demo scenarios + gRPC tests |
| 9. Observability | Week 5 | Monitoring and logging complete |

**Total Estimated Time**: 5 weeks

---

## 8. Getting Started - Quick Commands

### 8.1 Prerequisites
```bash
# Install required tools
- JDK 11 or higher
- sbt 1.9+
- Docker Desktop or Docker Engine + Docker Compose
- curl (for API testing)
```

### 8.2 Initialize Project
```bash
# Create project structure
mkdir -p multi-region-poc/{core,persistence,http,storage,integration}/src/main/scala
cd multi-region-poc

# Initialize sbt project
sbt new scala/scala3.g8
```

### 8.3 Start Local Multi-DC Environment
```bash
# Start all infrastructure (PostgreSQL + MinIO for 3 DCs)
docker-compose up -d

# Wait for services to be ready (30 seconds)
sleep 30

# Initialize MinIO buckets
./scripts/init-minio-buckets.sh

# Initialize PostgreSQL schemas
./scripts/init-postgres-schemas.sh

# Verify all services are running
docker-compose ps
```

### 8.4 Start Application Nodes
```bash
# Terminal 1 - Start us-east-1
sbt "project api" "run -Dconfig.file=us-east-1.conf"

# Terminal 2 - Start eu-west-1
sbt "project api" "run -Dconfig.file=eu-west-1.conf"

# Terminal 3 - Start ap-south-1
sbt "project api" "run -Dconfig.file=ap-south-1.conf"
```

### 8.5 Test the System

#### Basic Upload (Unrestricted)
```bash
# Upload a file to DC1 (no regional restrictions)
curl -X POST http://localhost:8081/api/files \
  -F "file=@./test-files/sample.pdf" \
  -F "owner=user@example.com"
# Response: {"fileId": "abc-123", "name": "sample.pdf", "allowedRegions": [], ...}

# Query from DC2 (after replication)
sleep 3
curl http://localhost:8082/api/files/abc-123

# Download from DC3 (should succeed - unrestricted)
curl http://localhost:8083/api/files/abc-123/download
```

#### Regional Restrictions Testing
```bash
# Upload EU-only file to DC2 (EU-West) - SHOULD SUCCEED
curl -X POST "http://localhost:8082/api/files?allowedRegions=eu-west-1" \
  -F "file=@./test-files/gdpr-data.pdf" \
  -F "owner=eu-user@example.com"
# Response: {"fileId": "eu-123", "allowedRegions": ["eu-west-1"], ...}

# Try to upload same restrictions to DC1 (US-East) - SHOULD FAIL
curl -X POST "http://localhost:8081/api/files?allowedRegions=eu-west-1" \
  -F "file=@./test-files/gdpr-data.pdf" \
  -F "owner=eu-user@example.com"
# Response 403: {
#   "error": "REGION_RESTRICTION_VIOLATED",
#   "message": "Upload not allowed in region 'us-east-1'. Allowed regions: ['eu-west-1']",
#   "currentRegion": "us-east-1"
# }

# Download from allowed region (DC2) - SHOULD SUCCEED
curl http://localhost:8082/api/files/eu-123/download
# Response 200: {"presignedUrl": "http://...", "s3Key": "..."}

# Download from forbidden region (DC1) - SHOULD FAIL
curl http://localhost:8081/api/files/eu-123/download
# Response 403: {
#   "error": "DOWNLOAD_NOT_AUTHORIZED",
#   "message": "Download not allowed from region 'us-east-1'. Allowed regions: ['eu-west-1']",
#   "currentRegion": "us-east-1"
# }

# Multi-region allowed file
curl -X POST "http://localhost:8081/api/files?allowedRegions=us-east-1,ap-south-1" \
  -F "file=@./test-files/multi-region.pdf" \
  -F "owner=global-user@example.com"
# Can download from DC1 (US-East) and DC3 (AP-South), but NOT DC2 (EU-West)
```

#### View in MinIO Console
```bash
open http://localhost:9100  # DC1 MinIO Console
open http://localhost:9101  # DC2 MinIO Console
open http://localhost:9102  # DC3 MinIO Console
# Login: minioadmin / minioadmin123
```

### 8.6 Run Demonstration Scenarios
```bash
# Run automated demo script
./scripts/demo.sh

# Run specific scenarios
./scripts/demo-concurrent-writes.sh
./scripts/demo-dc-failure.sh
./scripts/demo-regional-restrictions.sh  # New: Test regional compliance
```

### 8.7 Run Tests
```bash
# Unit tests
sbt test

# Integration tests (requires Docker Compose running)
sbt integration/test

# Full test suite
sbt clean coverage test integration/test coverageReport
```

### 8.8 Stop Environment
```bash
# Stop application nodes (Ctrl+C in each terminal)

# Stop infrastructure
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

---

## 9. Additional Resources

### Documentation
- [Apache Pekko Documentation](https://pekko.apache.org/docs/pekko/current/)
- [Pekko Persistence](https://pekko.apache.org/docs/pekko/current/typed/persistence.html)
- [Replicated Event Sourcing](https://pekko.apache.org/docs/pekko/current/typed/replicated-eventsourcing.html)
- [Pekko Cluster](https://pekko.apache.org/docs/pekko/current/typed/cluster.html)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [MinIO Docker Guide](https://min.io/docs/minio/container/index.html)

### Example Projects
- Pekko samples repository
- Event Sourcing CQRS examples

### Tools for Local Development
- [MinIO](https://min.io/) - S3-compatible object storage
- [LocalStack](https://localstack.cloud/) - Alternative AWS service emulator
- [Docker Compose](https://docs.docker.com/compose/) - Multi-container orchestration
- [pgAdmin](https://www.pgadmin.org/) - PostgreSQL management (optional)

---

## 10. Next Steps

1. **Review this plan** with your team
2. **Set up development environment**:
   ```bash
   # Install JDK 11+
   # Install sbt 1.9+
   # Install Docker Desktop
   docker --version  # Verify
   docker-compose --version
   ```
3. **Create Docker Compose file** (Phase 1):
   - 3 PostgreSQL containers
   - 3 MinIO containers
   - Network configuration
4. **Initialize the project structure** (Phase 1)
5. **Test infrastructure only**:
   ```bash
   docker-compose up -d
   # Verify MinIO at localhost:9090-9092
   # Verify PostgreSQL at localhost:5432-5434
   ```
6. **Start with domain model implementation** (Phase 2)
7. **Iterate and refine** based on learnings

---

## Notes

**Key Design Decisions:**
- **Local-First Approach**: This project is designed for local demonstration using Docker Compose
- **MinIO Over AWS S3**: Using MinIO for zero-cost local S3-compatible storage
- **3 Datacenters**: Demonstrates true multi-region behavior on a single machine
- **Simple Domain**: File Manager keeps focus on multi-DC infrastructure patterns
- **No Authentication Initially**: Can be added as Phase 9

**Development Philosophy:**
- Start with infrastructure working locally
- Build domain model with minimal complexity
- Focus on replication and consistency patterns
- Create comprehensive demo scenarios
- Document everything for easy onboarding

**Future Enhancements:**
- Security aspects (authentication, authorization) - Phase 9
- CI/CD pipeline setup - Phase 10
- Kubernetes deployment manifests - Phase 11
- Monitoring dashboards (Grafana/Prometheus) - Phase 12
- Real AWS deployment guide - Phase 13
