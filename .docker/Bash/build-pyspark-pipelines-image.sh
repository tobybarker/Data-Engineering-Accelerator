#!/bin/bash

# Getting the script root is different for MacOS / Linux
unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     SCRIPT_DIR=$(dirname "$0");;
    Darwin*)    SCRIPT_DIR=$( cd "$(dirname "$0")" ; pwd -P )
esac

DOCKER_DIR=$(dirname $SCRIPT_DIR)
INGESTION_PIPELINES_ROOT_DIR=$(dirname $DOCKER_DIR)

echo "Variables:"
echo "  SCRIPT_DIR=$SCRIPT_DIR"
echo "  DOCKER_DIR=$DOCKER_DIR"
echo "  INGESTION_PIPELINES_ROOT_DIR=$INGESTION_PIPELINES_ROOT_DIR"

docker build -t pyspark-pipelines -f $DOCKER_DIR/Dockerfile $INGESTION_PIPELINES_ROOT_DIR

# Clean up base images. Pipe output to null device as error's removing missing images are not useful
docker rmi -f python:3.8 > /dev/null 2>&1
docker rmi -f openjdk:8-jdk > /dev/null 2>&1