#!/bin/bash

############################################################
# Help                                                     #
############################################################
Help()
{
   # Display Help
   echo "Start the docker environment."
   echo
   echo "Syntax: scriptTemplate [-d|h]"
   echo "options:"
   echo "d     Start the container is disconnected mode. You can connect to the container using 'docker exec -it pyspark-pipeline-container /bin/sh'."
   echo "m     Max memory the container can use. Should be a number followed by b, k, m, g, to indicate bytes, kilobytes, megabytes, or gigabytes. Default is 4g."
   echo "h     Print this Help."
   echo
}

############################################################
# Main program                                             #
############################################################

# Getting the script root is different for MacOS / Linux
unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     SCRIPT_DIR=$(dirname "$0");;
    Darwin*)    SCRIPT_DIR=$( cd "$(dirname "$0")" ; pwd -P )
esac

DOCKER_DIR=$(dirname $SCRIPT_DIR)
PIPELINES_ROOT_DIR=$(dirname $DOCKER_DIR)
PIPELINES_PARENT_DIR=$(dirname $PIPELINES_ROOT_DIR)
MAX_MEMORY=4g

# Process options
# while getopts dh: flag
# do
while getopts ":hd" flag; do
    case "${flag}" in
        h) # display Help
         Help
         exit;;
        d) # Run container diconnected
         DOCKER_RUN_DISCONNECTED="YES";;
        m) # Max memory for the container
         MEMORY=${OPTARG};;
       \?) # Invalid option
         echo "Error: Invalid option"
         exit;;
    esac
done

echo "Variables:"
echo "  SCRIPT_DIR=$SCRIPT_DIR"
echo "  DOCKER_DIR=$DOCKER_DIR"
echo "  PIPELINES_ROOT_DIR=$PIPELINES_ROOT_DIR"
echo "  PIPELINES_PARENT_DIR=$PIPELINES_PARENT_DIR"
echo "  DOCKER_START_OPT=$DOCKER_START_OPT"
echo "  DOCKER_RUN_DISCONNECTED=$DOCKER_RUN_DISCONNECTED"
echo "  MAX_MEMORY=$MAX_MEMORY"
echo

# Stop & remove the container if it is already running. The output is either the container name, or a 
# message saying it's not found, so redirect the output to /dev/null
docker stop pyspark-pipeline-container > /dev/null 2>&1

# Sometimes the container takes a few seconds to come down properly when running VSCode connected
# to the container, so wait a few seconds...
sleep 3

# Start the container
if [ -z "$DOCKER_RUN_DISCONNECTED" ]; then
    echo "Entering container shell (Type 'exit' to stop the container)..."

    docker run -it \
    --name pyspark-pipeline-container \
    -m $MAX_MEMORY \
    -v $PIPELINES_PARENT_DIR/Pipelines/:/workspace/Pipelines/ \
    --init \
    --rm pyspark-pipelines
else
    docker run -dit \
    --name pyspark-pipeline-container \
    -m $MAX_MEMORY \
    -v $PIPELINES_PARENT_DIR/Pipelines/:/workspace/Pipelines/ \
    --init \
    --rm pyspark-pipelines
    
    docker ps
fi