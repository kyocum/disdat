#
# Kickstart the Python 2 system environment
#

FROM python:2.7.14-slim

LABEL \
	author="Theodore Wong"

# Kickstart shell scripts root
ARG KICKSTART_ROOT
ENV KICKSTART_ROOT $KICKSTART_ROOT

RUN apt-get update
RUN apt-get upgrade -y

# Install git and a minimal Python 2.x toolchain. Disdat uses git to detect
# changed sources when deciding whether or not to rerun a pipeline.
# disdat uses pyodbc which requires gcc ,hence 'build-essential'
# sometimes people need to install .deb files, hence gdebi
RUN apt-get install -y git build-essential unixodbc-dev
RUN easy_install virtualenv

# Install the kickstart scripts used by later layers
COPY kickstart $KICKSTART_ROOT

# Declare Miniconda configurable arguments. We only need to save the Python
# virtual environment path for later stages.
ARG VIRTUAL_ENV
ENV VIRTUAL_ENV $VIRTUAL_ENV

