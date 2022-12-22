# Redkite Engineering Refresher

## Introduction

- The goal for this repo is two fold:
  - Provide the foundations of an extensible, production worthy PySpark repository
  - Achieve the pipeline requirements provided
- The bare bones have been built for you, but development is needed

## Getting Started

1. Clone this repo onto your local machine
2. Install Docker on your local machine
3. Create the docker image by running either the [Powershell build file](./.docker/Powershell/build-redkite-refresher-pyspark-image.ps1) or the [Bash build file](./.docker/Bash/build-pyspark-pipelines-image.sh)
4. With the image created, spin up a container using either the [Powershell container file](./.docker/Powershell/start-redkite-refresher-pyspark-container.ps1) or the [Bash container file](./.docker/Bash/start-pyspark-pipelines-container.sh)
5. Attach to the running container, if using Visual Studio Code the Remote Explorer extension is recommended
6. Select Python Interpreter and choose the 3.8 version of Python
7. Run `main.py`
8. Once you have the example job running successfully, review and complete the steps listed in the [Tasks document](wiki/1%20Tasks.md)

## Repo Structure

- `main.py` is the single entry point to trigger jobs
- By using a single file as an entry point, when triggering jobs via Azure Data Factory we can use a generic job that runs `main.py`, and use parameters (`job_path`) to determine which job should be run
- An `example_arg` parameter is used to provide an example of how arguments can be passed at run time through to the job being triggered. This example argument can be expanded upon to achieve the requirements


## VSCode Debugging

- `launch.json` has been setup to run `main.py` passing the following parameters `job_path: example_job.py`, `example_arg: hello_world`
- These parameters can be changed to trigger other job files, and to pass other arguments

## Notes

- Review the [Redkite Data Engineering Documentation](https://dataengineering.redkite.com)
- Feel free to use either OOP or Functional Programming to achieve the business requirements
