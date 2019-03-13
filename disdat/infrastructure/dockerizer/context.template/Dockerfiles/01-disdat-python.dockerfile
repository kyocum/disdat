#
# Install Disdat in the Python virtual environment.
#

ARG IMAGE_LAYER
FROM $IMAGE_LAYER

LABEL \
	author="Theodore Wong"

# Temporary build directory
ARG BUILD_ROOT
ENV BUILD_ROOT $BUILD_ROOT

# Name of curent Disdat Sdist
ARG DISDAT_SDIST

# Copy the Disdat source to the temporary build root
COPY disdat $BUILD_ROOT/disdat

# ...and install Disdat
RUN virtualenv $VIRTUAL_ENV

# UGH.   setuptools 40.7.0 breaks pyodbc installs by passing in unicode to distutils and
# it breaks looking for a type StringType, and not creating an array and then breaking when string doesn't have append
# if 2.7.x-slim
RUN ["/bin/bash", "-c", "source $VIRTUAL_ENV/bin/activate; pip install setuptools==40.6.0; pip install --no-cache-dir $BUILD_ROOT/disdat/dockerizer/context.template/$DISDAT_SDIST; deactivate"]

# Use this line when we move to P3.
# RUN ["/bin/bash", "-c", "source $VIRTUAL_ENV/bin/activate; pip install $BUILD_ROOT/disdat/dockerizer/context.template/$DISDAT_SDIST; deactivate"]

# Add the virtual environment Python to the head of the PATH; running
# `python` will then get you the installed virtual environment and the
# `dsdt` command-line executable.
ENV PATH $VIRTUAL_ENV/bin:$PATH

# Initialize the Disdat environment
RUN dsdt init

# Local environment may have its own pip index, support pip.conf files, if not set, copies empty file.
COPY pip.conf /opt/pip.conf
ENV PIP_CONFIG_FILE /opt/pip.conf

# Local environmment may have its own odbc.ini file, if not set, copies empty file.
COPY odbc.ini /opt/odbc.ini
ENV ODBCINI /opt/odbc.ini