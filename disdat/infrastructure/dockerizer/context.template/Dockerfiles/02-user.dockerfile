#
# Kickstart the user-defined execution environment. This includes operating
# system dependencies and Python requirements.
#

ARG IMAGE_LAYER
FROM $IMAGE_LAYER

LABEL \
	author="Theodore Wong"

# Copy the user configuration files
COPY config $BUILD_ROOT/config

# Install the user operating system dependencies
# TODO: We probably ought to replace this with a script that checks the
# underlying O/S type and then selects the correct O/S package list.
ARG OS_NAME
RUN echo $OS_NAME
RUN if [ -f $BUILD_ROOT/config/$OS_NAME/repository.txt ]; then \
	for repo in $(cat $BUILD_ROOT/config/$OS_NAME/repository.txt); do \
		add-apt-repository -y $repo; \
	done; \
	apt-get update -y; \
fi
RUN if [ -f $BUILD_ROOT/config/$OS_NAME/deb.txt ]; then \
	apt-get install -y $(cat $BUILD_ROOT/config/$OS_NAME/deb.txt); \
fi
RUN files=$(echo $BUILD_ROOT/config/$OS_NAME/*.deb); if [ "$files" != $BUILD_ROOT/config/$OS_NAME/'*.deb' ]; then \
	for i in $files; do echo "Installing $i..."; dpkg -i $i; apt-get install -y -f;  done; \
fi

# Install R and packages
RUN if [ -f $BUILD_ROOT/config/$OS_NAME/r.txt ]; then \
    apt-get update -y \
    && apt-get install -y --no-install-recommends --no-install-suggests apt-transport-https ca-certificates software-properties-common gnupg2 gnupg1 \
    && apt-key add $BUILD_ROOT/config/$OS_NAME/r-debian.pub \
    && add-apt-repository "deb https://cloud.r-project.org/bin/linux/debian buster-cran35/" \
    && apt-get update -y \
    && apt-get upgrade -y \
    && lsb_release -a \
    && apt-get install -y --no-install-recommends --allow-unauthenticated \
         r-base \
         r-base-dev \
         libssl-dev \
         libcurl4-openssl-dev; \
	for pkg in $(cat $BUILD_ROOT/config/$OS_NAME/r.txt); do \
		R -e "install.packages('$pkg', repos='https://cloud.r-project.org/')"; \
	done; \
fi

# Install user Python sdist package dependencies
# NOTE: Since PIP 19.0 fails with --no-cache-dir, removed '-n' flag on kickstart-python.py script
RUN files=$(echo $BUILD_ROOT/config/python-sdist/*.tar.gz); if [ "$files" != $BUILD_ROOT/config/python-sdist/'*.tar.gz' ]; then \
	$KICKSTART_ROOT/bin/kickstart-python.sh $VIRTUAL_ENV $i; \
	for i in $files; do \
		$KICKSTART_ROOT/bin/install-python-package-from-source-tree.sh $VIRTUAL_ENV $i; \
	done; \
fi

# NOTE: 01-disdat-python.dockerfile has set the PATH using ENV, so $VIRTUAL_ENV is already activated.

# Install the user's package.   Split into two parts: package dependencies and user code.
# First, the Makefile creates egginfo, to create a requires.txt file, and copies into context
# We only issue the COPY for that file.  This will invalidate the docker layer cache if the requires.txt changes.
# Second, we install the sdist *.tar.gz.  B/c sdists change on each create, that's installed every time.
ARG PIPELINE_ROOT
COPY pipeline/user_package_egg_requires.txt $PIPELINE_ROOT/
RUN pip install `sed -n '1,/\[.*\]/p' $PIPELINE_ROOT/user_package_egg_requires.txt | grep -v '\[' | awk 'NF'`

COPY pipeline $PIPELINE_ROOT
RUN pip install --no-cache-dir $PIPELINE_ROOT/*.tar.gz

# Set the git status env variables for the container. This represents the most recent git hash for the pipeline.
ARG GIT_HASH
ENV PIPELINE_GIT_HASH=${GIT_HASH}
ARG GIT_BRANCH
ENV PIPELINE_GIT_BRANCH=${GIT_BRANCH}
ARG GIT_FETCH_URL
ENV PIPELINE_GIT_FETCH_URL=${GIT_FETCH_URL}
ARG GIT_TIMESTAMP
ENV PIPELINE_GIT_TIMESTAMP=${GIT_TIMESTAMP}
ARG GIT_DIRTY
ENV PIPELINE_GIT_DIRTY=${GIT_DIRTY}

# Clean up the temporary build directory
RUN rm -rf $BUILD_ROOT

# Set up the default entry point. If the user starts an image with no
# arguments, show some help
COPY bin/entrypoint.py /opt/bin/entrypoint.py
CMD [ "/opt/bin/entrypoint.py", "--help" ]
