#
# Kickstart the Ubuntu 16.04 operating system environment
#

FROM ubuntu:16.04

LABEL \
	author="Theodore Wong"

# Kickstart shell scripts root
ARG KICKSTART_ROOT
ENV KICKSTART_ROOT $KICKSTART_ROOT

# Installation in Docker images is noninteractive
ENV DEBIAN_FRONTEND noninteractive

# Install apt-utils to stop subsequent errors of the form:
# debconf: delaying package configuration, since apt-utils is not installed
# Also install gdebi to make installing arbitrary .deb files with
# dependencies less painful
RUN apt-get update -y && apt-get install -y --no-install-recommends apt-utils gdebi software-properties-common
RUN apt-get upgrade -y

# Install git and a minimal Python 2.x toolchain. Disdat uses git to detect
# changed sources when deciding whether or not to rerun a pipeline.
RUN apt-get install -y git python python-pip python-virtualenv

# Install the kickstart scripts used by later layers
COPY kickstart $KICKSTART_ROOT

# Declare Miniconda configurable arguments. We only need to save the Python
# virtual environment path for later stages.
ARG CONDA_VERSION
ARG VIRTUAL_ENV
ENV VIRTUAL_ENV $VIRTUAL_ENV

# Install Miniconda if selected.
RUN if [ x$CONDA_VERSION != xNO_CONDA ]; then \
	$KICKSTART_ROOT/bin/kickstart-conda.sh -c $CONDA_VERSION $VIRTUAL_ENV; \
fi
