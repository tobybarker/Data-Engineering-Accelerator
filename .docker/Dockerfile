#-------------------------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See https://go.microsoft.com/fwlink/?linkid=2090316 for license information.
#-------------------------------------------------------------------------------------------------------------
FROM openjdk:8-jdk
COPY --from=python:3.8 / /

# This Dockerfile adds a non-root user with sudo access. Use the "remoteUser"
# property in devcontainer.json to use it. On Linux, the container user's GID/UIDs
# will be updated to match your local UID/GID (when using the dockerFile property).
# See https://aka.ms/vscode-remote/containers/non-root-user for details.
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG DBCVER

# Configure apt
ENV DEBIAN_FRONTEND=noninteractive

COPY .docker/requirements.txt .

RUN apt-get update --fix-missing \
    && apt-get install -y wget bzip2 ca-certificates curl git build-essential bash bash-completion \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
	&& pip install --upgrade pip \
	&& pip install -r requirements.txt \
# VSCode DevContainers
	&& apt-get update \
    && apt-get -y install --no-install-recommends apt-utils dialog 2>&1 \
# Create a non-root user to use if preferred - see https://aka.ms/vscode-remote/containers/non-root-user.
    && groupadd --gid $USER_GID $USERNAME \
    && useradd -s /bin/bash --uid $USER_UID --gid $USER_GID -m $USERNAME \
# [Optional] Add sudo support for the non-root user
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME\
    && chmod 0660 /etc/sudoers.d/$USERNAME \
 # Verify git, needed tools installed
    && apt-get -y install git iproute2 procps curl lsb-release \
# Clean up
	&& apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/* \
	# Allow for a consistant java home location for settings - image is changing over time
	&& if [ ! -d "/docker-java-home" ]; then ln -s "${JAVA_HOME}" /docker-java-home; fi

ENTRYPOINT /bin/bash
# CMD  

ENV DEBIAN_FRONTEND=dialog