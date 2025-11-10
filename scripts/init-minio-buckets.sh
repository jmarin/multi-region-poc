#!/bin/bash

# Script to initialize MinIO buckets for all regions
set -e

echo "Initializing MinIO buckets for all regions..."

# Install mc (MinIO client) if not available
if ! command -v mc &> /dev/null; then
    echo "Installing MinIO client (mc)..."
    curl -o /tmp/mc https://dl.min.io/client/mc/release/linux-amd64/mc
    chmod +x /tmp/mc
    sudo mv /tmp/mc /usr/local/bin/
fi

# Configure MinIO clients for each region
echo "Configuring MinIO clients..."
mc alias set us-east-1 http://localhost:9000 minioadmin minioadmin123
mc alias set eu-west-1 http://localhost:9001 minioadmin minioadmin123
mc alias set ap-south-1 http://localhost:9002 minioadmin minioadmin123

# Create buckets
echo "Creating buckets..."
mc mb us-east-1/filemanager-us-east-1 --ignore-existing
mc mb eu-west-1/filemanager-eu-west-1 --ignore-existing
mc mb ap-south-1/filemanager-ap-south-1 --ignore-existing

# Set public policy (for demo purposes only)
echo "Setting bucket policies..."
mc anonymous set download us-east-1/filemanager-us-east-1
mc anonymous set download eu-west-1/filemanager-eu-west-1
mc anonymous set download ap-south-1/filemanager-ap-south-1

echo "✓ MinIO buckets initialized successfully!"
echo ""
echo "MinIO Console URLs:"
echo "  - US-East-1: http://localhost:9100"
echo "  - EU-West-1: http://localhost:9101"
echo "  - AP-South-1: http://localhost:9102"
echo ""
echo "Login credentials: minioadmin / minioadmin123"
