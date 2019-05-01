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

# NOTE: We were installing gdebi in the slim.dockerfile.  It includes an enormous number of dependencies.
# We can't use autoremove, since it removes more than we installed.   The below is one way to
# use gdebi, only installing if they have actual .deb files to install.
#	apt-get install -y gdebi; \
#	for i in $files; do echo "Installing $i..."; gdebi -n $i; done; \
#	apt-get -y purge gdebi; \


# Install user Python sdist package dependencies
# NOTE: Since PIP 19.0 fails with --no-cache-dir, removed '-n' flag on kickstart-python.py script
# NOTE: need to test with Python 3.6+
RUN files=$(echo $BUILD_ROOT/config/python-sdist/*.tar.gz); if [ "$files" != $BUILD_ROOT/config/python-sdist/'*.tar.gz' ]; then \
	for i in $files; do \
		$KICKSTART_ROOT/bin/kickstart-python.sh $VIRTUAL_ENV $i; \
		$KICKSTART_ROOT/bin/install-python-package-from-source-tree.sh $VIRTUAL_ENV $i; \
	done; \
fi

# Install the pipeline package. The user must have a valid setup.py that can generate an sdist .tar.gz.
# That will have been copied into the docker context.  Note, the install-python-package-from-source-tree script
# installs with '--no-cache-dir' option.
ARG PIPELINE_ROOT
COPY pipeline $PIPELINE_ROOT
RUN files=$(echo $PIPELINE_ROOT/*.tar.gz);  if [ "$files" != $PIPELINE_ROOT/'*.tar.gz' ]; then \
	for i in $files; do \
	    echo $i; \
    	$KICKSTART_ROOT/bin/install-python-package-from-source-tree.sh $VIRTUAL_ENV $i; \
    done; \
fi

# Clean up the temporary build directory
RUN rm -rf $BUILD_ROOT

# Set up the default entry point. If the user starts an image with no
# arguments, show some help
COPY bin/entrypoint.py /opt/bin/entrypoint.py
ENTRYPOINT [ "/opt/bin/entrypoint.py" ]
CMD [ "--help" ]
