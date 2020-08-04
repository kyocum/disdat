#
# Kickstart the Python 2 system environment
#

FROM python:3.7.8-slim

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
RUN pip install virtualenv==16.7.9

# Since Ubuntu 20.x the default security level
# has been raised to 2.   This causes this problem:
# https://askubuntu.com/questions/1231844/ssl-sslerror-ssl-dh-key-too-small-dh-key-too-small-ssl-c1108
# NOTE: This should be a change at the offending server, not the client.

RUN mv /etc/ssl/openssl.cnf /etc/ssl/openssl.back.cnf; \
    sed -e 's/DEFAULT@SECLEVEL=2/DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.back.cnf > /etc/ssl/openssl.cnf

# Install the kickstart scripts used by later layers
COPY kickstart $KICKSTART_ROOT

# Declare Miniconda configurable arguments. We only need to save the Python
# virtual environment path for later stages.
ARG VIRTUAL_ENV
ENV VIRTUAL_ENV $VIRTUAL_ENV

