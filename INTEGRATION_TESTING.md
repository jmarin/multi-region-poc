# Integration Testing Guide

This guide explains how to run integration tests for the File Manager REST API.

## Prerequisites

- Docker and Docker Compose installed
- SBT installed
- Ports 5432, 9000-9002, 9100-9102 available

## Infrastructure Setup

### 1. Start Infrastructure Services

Start PostgreSQL and MinIO containers:

```bash
docker-compose up -d
```

This will start:
- PostgreSQL databases for each region (ports 5433-5435)
- MinIO instances for each region (ports 9000-9002 for API, 9100-9102 for console)
- MinIO initialization container (creates buckets automatically)

### 2. Verify Services are Running

Check that all containers are healthy:

```bash
docker-compose ps
```

You should see all services in "Up" state with "(healthy)" status.

### 3. Access MinIO Console

You can access the MinIO web console for each region:

- US-EAST-1: http://localhost:9100
- EU-WEST-1: http://localhost:9101
- AP-SOUTH-1: http://localhost:9102

Login credentials:
- Username: `minioadmin`
- Password: `minioadmin123`

### 4. Check Database

Connect to PostgreSQL:

```bash
docker exec -it filemanager-postgres-us-east-1 psql -U filemanager -d filemanager_us_east_1
```

Verify tables were created:
```sql
\dt
```

You should see tables: `event_journal`, `snapshot`, `event_tag`, `projection_offset`

## Running the REST API

### Option 1: Run from SBT

```bash
# Set the region (optional, defaults to us-east-1)
export REGION_ID=us-east-1

# Run the API
sbt "api/run"
```

The API will start on http://localhost:8080

### Option 2: Run with Custom Configuration

Create a custom `application.conf`:

```bash
sbt "api/run -Dconfig.file=/path/to/custom.conf"
```

### Access Swagger UI

Once the API is running, open http://localhost:8080/docs in your browser to see the interactive API documentation.

## Running Integration Tests

### 1. Ensure Infrastructure is Running

```bash
docker-compose ps
```

### 2. Start the REST API

In one terminal:
```bash
sbt "api/run"
```

Wait for the server to start (you'll see "Server started at..." message).

### 3. Run Tests

In another terminal:
```bash
sbt "integration/test"
```

## Manual API Testing

### Upload a File

```bash
curl -X POST http://localhost:8080/files/upload \
  -F "file=@/path/to/your/file.txt" \
  -F "owner=testuser"
```

### Get File Info

```bash
curl http://localhost:8080/files/{fileId}
```

### Download File

```bash
curl http://localhost:8080/files/{fileId}/download
```

This returns a presigned URL that you can use to download the file directly from MinIO.

### List Files

```bash
curl "http://localhost:8080/files/list?owner=testuser&limit=10"
```

### Delete File

```bash
curl -X DELETE http://localhost:8080/files/{fileId}
```

### Health Check

```bash
curl http://localhost:8080/health
```

## Testing Multi-Region

To test multi-region replication:

### 1. Start Multiple API Instances

Terminal 1 (US-EAST-1):
```bash
export REGION_ID=us-east-1
sbt "api/run"
```

Terminal 2 (EU-WEST-1):
```bash
export REGION_ID=eu-west-1
export GRPC_PORT=9091
sbt "api/run -Dfilemanager.http.port=8081"
```

Terminal 3 (AP-SOUTH-1):
```bash
export REGION_ID=ap-south-1
export GRPC_PORT=9092
sbt "api/run -Dfilemanager.http.port=8082"
```

### 2. Upload to One Region

```bash
curl -X POST http://localhost:8080/files/upload \
  -F "file=@test.txt" \
  -F "owner=testuser"
```

### 3. Check Replication

After a few seconds, check if the file is visible from other regions:

```bash
# EU-WEST-1
curl http://localhost:8081/files/list?owner=testuser

# AP-SOUTH-1
curl http://localhost:8082/files/list?owner=testuser
```

## Cleanup

### Stop Infrastructure

```bash
docker-compose down
```

### Remove Volumes (Clean Slate)

```bash
docker-compose down -v
```

This will delete all data (databases and S3 files).

## Troubleshooting

### MinIO Buckets Not Created

If buckets weren't created automatically:

```bash
# Enter MinIO client container
docker-compose run --rm minio-init
```

### Database Connection Issues

Check if PostgreSQL is accepting connections:

```bash
docker logs filemanager-postgres-us-east-1
```

### Port Already in Use

If ports are already in use, modify `docker-compose.yml` to use different ports.

### API Not Starting

Check logs:
```bash
# In SBT terminal, you'll see logs
# Or check application logs if running as service
```

Common issues:
- MinIO not accessible (check endpoint in application.conf)
- Database not accessible (check JDBC URL)
- Pekko cluster not forming (check seed nodes configuration)

## Environment Variables

You can override configuration with environment variables:

- `REGION_ID`: Region identifier (us-east-1, eu-west-1, ap-south-1)
- `JDBC_URL`: PostgreSQL connection URL
- `JDBC_USER`: Database username
- `JDBC_PASSWORD`: Database password
- `MINIO_US_EAST_1_ENDPOINT`: MinIO endpoint for US-EAST-1
- `MINIO_EU_WEST_1_ENDPOINT`: MinIO endpoint for EU-WEST-1
- `MINIO_AP_SOUTH_1_ENDPOINT`: MinIO endpoint for AP-SOUTH-1
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key
- `MAX_FILE_SIZE`: Maximum file size in bytes
- `GRPC_PORT`: gRPC server port

## Next Steps

1. Implement actual HTTP clients in integration tests
2. Add load testing with Gatling
3. Add monitoring with Prometheus/Grafana
4. Deploy to Kubernetes for true multi-region testing
