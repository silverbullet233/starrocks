#!/bin/bash

set -e

IMAGE_NAME="starrocks-dev:ci"
CONTAINER_NAME="xujia-ci-test"
BASE_IMAGE="172.26.92.142:5000/starrocks/dev-env-ubuntu:latest"

# Get current user's UID and GID
USER_ID=$(id -u)
GROUP_ID=$(id -g)
USERNAME=$(whoami)

echo "Building Docker image with user ${USERNAME} (UID: ${USER_ID}, GID: ${GROUP_ID})..."

# Build the custom image
docker build \
    --build-arg USERNAME=${USERNAME} \
    --build-arg USER_ID=${USER_ID} \
    --build-arg GROUP_ID=${GROUP_ID} \
    -t ${IMAGE_NAME} \
    -f Dockerfile.ci \
    .

echo ""
echo "Image built successfully: ${IMAGE_NAME}"
echo ""

# Check if container already exists
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Removing existing container: ${CONTAINER_NAME}"
    docker rm -f ${CONTAINER_NAME}
fi

# Create and start the container
echo "Creating and starting container: ${CONTAINER_NAME}"
docker run -it --name ${CONTAINER_NAME} ${IMAGE_NAME} bash
