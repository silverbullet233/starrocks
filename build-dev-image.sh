#!/bin/bash

# Script to build Docker image from my-dev-env.Dockerfile and tag it as mxr-starrocks-dev:latest

set -e  # Exit immediately if a command exits with a non-zero status

# Default tag
DEFAULT_TAG="mxr-starrocks-dev:latest"

# Check if a custom tag was provided as an argument
if [ $# -eq 0 ]; then
    TAG="$DEFAULT_TAG"
    echo "No custom tag provided. Using default tag: $TAG"
else
    TAG="$1"
    echo "Using custom tag: $TAG"
fi

echo "Building Docker image from my-dev-env.Dockerfile..."

# Check if Dockerfile exists
if [ ! -f "my-dev-env.Dockerfile" ]; then
    echo "Error: my-dev-env.Dockerfile not found in current directory!"
    exit 1
fi

# Build the Docker image with the specified Dockerfile and tag it
docker build -f my-dev-env.Dockerfile -t "$TAG" .

echo "Docker image built and tagged as $TAG"

# If using the default tag, also tag with a timestamp
if [ "$TAG" = "$DEFAULT_TAG" ]; then
    TIMESTAMP_TAG="mxr-starrocks-dev:$(date +%Y%m%d-%H%M%S)"
    docker tag "$DEFAULT_TAG" "$TIMESTAMP_TAG"
    echo "Also tagged as $TIMESTAMP_TAG"
fi

echo "Build completed successfully!"