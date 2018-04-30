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

RUN $KICKSTART_ROOT/bin/kickstart-python.sh -n $VIRTUAL_ENV
RUN $KICKSTART_ROOT/bin/install-python-package-from-source-tree.sh $VIRTUAL_ENV $BUILD_ROOT/disdat/dockerizer/context.template/$DISDAT_SDIST

# Add the virtual environment Python to the head of the PATH; running
# `python` will then get you the installed virtual environment and the
# `dsdt` command-line executable.
ENV PATH $VIRTUAL_ENV/bin:$PATH

# Initialize the Disdat environment
RUN dsdt init
