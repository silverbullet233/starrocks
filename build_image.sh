#!/bin/bash

# Script to build Docker image from my-dev-env.Dockerfile
# Creates image named mxr-starrocks-dev with latest tag

set -e  # Exit immediately if a command exits with a non-zero status

echo "Building Docker image from my-dev-env.Dockerfile..."

# Check if Docker is installed and running
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed or not in PATH"
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "Error: Docker daemon is not running"
    exit 1
fi

# Check if the Dockerfile exists
DOCKERFILE_PATH="my-dev-env.Dockerfile"
if [ ! -f "$DOCKERFILE_PATH" ]; then
    echo "Error: $DOCKERFILE_PATH not found in current directory"
    exit 1
fi

# Build the Docker image with specified name and tag
echo "Building image: mxr-starrocks-dev:latest"
docker build -f "$DOCKERFILE_PATH" -t mxr-starrocks-dev:latest .

# Check if the build was successful
if [ $? -eq 0 ]; then
    echo "Successfully built image: mxr-starrocks-dev:latest"
    
    # Show the image information
    echo "Image details:"
    docker images mxr-starrocks-dev:latest
else
    echo "Failed to build the Docker image"
    exit 1
fi