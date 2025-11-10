# Quick Start Guide

Get the File Manager REST API running in minutes!

## 1. Start Infrastructure (1 minute)

```bash
# Start PostgreSQL and MinIO containers
docker-compose up -d

# Wait for services to be healthy (30 seconds)
docker-compose ps
```

## 2. Start the API (30 seconds)

```bash
# Start the REST API server
sbt "api/run"
```

Wait for the message: "Server started at..."

## 3. Test the API (30 seconds)

### Check Health
```bash
curl http://localhost:8080/health
```

### View API Documentation
Open http://localhost:8080/docs in your browser

### Upload a File
```bash
# Create a test file
echo "Hello, File Manager!" > test.txt

# Upload it
curl -X POST http://localhost:8080/files/upload \
  -F "file=@test.txt" \
  -F "owner=myuser"
```

You'll get a response like:
```json
{
  "fileId": "a1b2c3d4-...",
  "fileName": "test.txt",
  "contentType": "text/plain",
  "contentLength": 21,
  "s3Key": "us-east-1/a1b2c3d4-...",
  "status": "uploaded",
  "message": "File test.txt uploaded successfully to us-east-1"
}
```

### List Files
```bash
curl "http://localhost:8080/files/list?owner=myuser"
```

### Get File Info
```bash
# Use the fileId from upload response
curl "http://localhost:8080/files/{fileId}"
```

### Download File
```bash
# Get presigned download URL
curl "http://localhost:8080/files/{fileId}/download"

# Use the downloadUrl from response to download
curl "{downloadUrl}" -o downloaded-test.txt
```

### Delete File
```bash
curl -X DELETE "http://localhost:8080/files/{fileId}"
```

## What's Running?

| Service | Port | URL |
|---------|------|-----|
| REST API | 8080 | http://localhost:8080 |
| Swagger UI | 8080 | http://localhost:8080/docs |
| PostgreSQL | 5433 | localhost:5433 |
| MinIO (US-EAST-1) | 9000 | http://localhost:9000 |
| MinIO Console | 9100 | http://localhost:9100 |
| gRPC Server | 9090 | localhost:9090 |

## Configuration

The API uses these default settings (from `application.conf`):

```hocon
filemanager {
  http {
    interface = "0.0.0.0"
    port = 8080
  }
  
  validation {
    max-file-size = 104857600  # 100 MB
    allowed-content-types = [
      "image/jpeg", "image/png", "application/pdf", 
      "text/plain", "application/json", ...
    ]
  }
}

s3 {
  us-east-1 {
    endpoint = "http://localhost:9000"
    bucket = "filemanager-us-east-1"
    access-key = "minioadmin"
    secret-key = "minioadmin123"
  }
}
```

## Features

✅ **File Upload** - Upload files with automatic S3 storage and metadata tracking  
✅ **File Download** - Generate presigned URLs for secure downloads  
✅ **File Metadata** - Track file information, owner, timestamps, replicas  
✅ **File Listing** - List files by owner with pagination  
✅ **File Deletion** - Delete files from storage and metadata  
✅ **Validation** - File size and content type validation  
✅ **Multi-Region** - Support for multiple geographic regions  
✅ **Event Sourcing** - All operations persisted with Pekko Persistence  
✅ **REST API** - Type-safe API with Tapir + Http4s  
✅ **Swagger Docs** - Interactive API documentation

## Next Steps

- **Multi-Region Testing**: See [INTEGRATION_TESTING.md](INTEGRATION_TESTING.md)
- **API Details**: See [PHASE6_COMPLETE.md](PHASE6_COMPLETE.md)
- **Architecture**: See [README.md](README.md)

## Cleanup

Stop and remove everything:

```bash
# Stop services
docker-compose down

# Remove data volumes (clean slate)
docker-compose down -v
```

## Troubleshooting

### Port Already in Use

```bash
# Check what's using port 8080
lsof -i :8080

# Or start on different port
sbt "api/run -Dfilemanager.http.port=8081"
```

### MinIO Not Accessible

```bash
# Check MinIO status
docker logs filemanager-minio-us-east-1

# Restart MinIO
docker-compose restart minio-us-east-1
```

### Database Connection Failed

```bash
# Check PostgreSQL status
docker logs filemanager-postgres-us-east-1

# Verify database exists
docker exec -it filemanager-postgres-us-east-1 psql -U filemanager -d filemanager_us_east_1 -c '\dt'
```

## Have Questions?

- Check the full documentation in [README.md](README.md)
- Review integration testing guide in [INTEGRATION_TESTING.md](INTEGRATION_TESTING.md)
- See implementation details in [PHASE6_COMPLETE.md](PHASE6_COMPLETE.md)
