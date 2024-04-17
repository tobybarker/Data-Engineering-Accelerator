docker build -t redkite-refresher-pyspark -f $PSScriptRoot/../Dockerfile $PSScriptRoot/../../
docker rmi -f python:3.8
docker rmi -f openjdk:8-jdk