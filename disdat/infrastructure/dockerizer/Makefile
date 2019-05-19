# If the user provides a configuration file with build variables, use it
ifneq ($(CONFIG_MK), )
include $(CONFIG_MK)
endif

# The desired name for the pipeline Docker image
ifeq ($(PIPELINE_IMAGE_NAME), )
$(error PIPELINE_IMAGE_NAME variable not set)
endif

# The root for the Docker context in which we will build the image
DOCKER_CONTEXT := $(shell echo $(DOCKER_CONTEXT) | sed 's/\/*$$//')
ifeq ($(DOCKER_CONTEXT), )
$(error DOCKER_CONTEXT variable not set or invalid)
endif

# The root of the Disdat source distribution
DISDAT_ROOT := $(shell echo $(DISDAT_ROOT) | sed 's/\/*$$//')
ifeq ($(DISDAT_ROOT), )
$(error DISDAT_ROOT variable not set or invalid)
endif

# The current Disdat source distribution path
DISDAT_SDIST := $(shell basename `find $(DISDAT_ROOT)/disdat/infrastructure/dockerizer/context.template/*.tar.gz -type f`)

# The root of an extra user configuration files (O/S package dependencies,
# etc.)
CONFIG_ROOT ?= $(DISDAT_ROOT)/disdat/infrastructure/dockerizer/config
CONFIG_ROOT := $(shell echo $(CONFIG_ROOT) | sed 's/\/*$$//')
ifeq ($(CONFIG_ROOT), )
$(error CONFIG_ROOT variable not set or invalid)
endif

# The root of the Docker context template (Dockerfiles, kickstart scripts,
# etc.)
DOCKER_CONTEXT_TEMPLATE ?= $(DISDAT_ROOT)/disdat/infrastructure/dockerizer/context.template
DOCKER_CONTEXT_TEMPLATE := $(shell echo $(DOCKER_CONTEXT_TEMPLATE) | sed 's/\/*$$//')
ifeq ($(DOCKER_CONTEXT_TEMPLATE), )
$(error DOCKER_CONTEXT_TEMPLATE variable not set or invalid)
endif

# The source root of the pipeline to run
PIPELINE_ROOT := $(shell echo $(PIPELINE_ROOT) | sed 's/\/*$$//')
ifeq ($(PIPELINE_ROOT), )
$(error PIPELINE_ROOT variable not set or invalid)
endif

# Declare the base operating system to use
ifeq ($(OS_TYPE), )
$(error OS_TYPE variable not set)
endif
ifeq ($(OS_VERSION), )
$(error OS_VERSION variable not set)
endif
OS_NAME := $(OS_TYPE)-$(OS_VERSION)

# The Docker registry to which to push images (default is no registry)
DOCKER_REGISTRY ?=
# Flag to set if the Docker registry requires an AWS ECR login
DOCKER_REGISTRY_IS_ECR ?=

#
# Define build arguments to pass through when "running" the Docker
# dockerfiles. The IMAGE_ prefix flags all such arguments.
#

# Kickstart script installation
IMAGE_KICKSTART_ROOT ?= /opt/kickstart

# Directory for the Disdat Python virtual environment. By default, we
# install whatever packages show up when using pip. If the user wants
# Miniconda, they should set IMAGE_CONDA_VERSION in a custom CONFIG_MK
# file.
IMAGE_VIRTUAL_ENV ?= /opt/python-virtualenv
IMAGE_CONDA_VERSION ?= NO_CONDA

# Temporary build directory for holding various things copied in from the
# build context
IMAGE_BUILD_ROOT ?= /opt/build

# Permanent directory for holding the pipeline package source. Even though
# we install the pipeline package in Python virtual environment, we keep
# the source after the build in case it contains user data that is not
# otherwise installed by setuptools.
IMAGE_PIPELINE_ROOT ?= /opt/pipeline

#
# Internal Docker image layers
#

# Layer 00: Base operating system environment
IMAGE_00_LAYER := disdat-$(OS_NAME)
# Layer 01: Disdat and its Python package dependencies
IMAGE_01_LAYER := disdat-$(OS_NAME)-python

.PHONY: build build-00 build-01 build-02 context push

all: build

#
# Build the Docker images layers:
#
# Layer 00: Base operating system environment
# Layer 01: Disdat and its Python package dependencies
# Layer 02: The user operating system dependencies, Python requirements,
#           and scripts/executables
#

build: $(DOCKER_CONTEXT) build-00 build-01 build-02
	@echo "----- Built Docker image for the $(PIPELINE_IMAGE_NAME) pipeline on $(OS_NAME)"

sagemaker: build build-03
	@echo "----- Built SageMaker Docker image for the $(PIPELINE_IMAGE_NAME) pipeline on $(OS_NAME)"

build-00: $(DOCKER_CONTEXT)
	@echo "---------- Building base operating system environment"
	docker build \
		--build-arg KICKSTART_ROOT=$(IMAGE_KICKSTART_ROOT) \
		--build-arg CONDA_VERSION=$(IMAGE_CONDA_VERSION) \
		--build-arg VIRTUAL_ENV=$(IMAGE_VIRTUAL_ENV) \
		--file $(DOCKER_CONTEXT)/Dockerfiles/00-disdat-$(OS_NAME).dockerfile \
		--tag $(IMAGE_00_LAYER) \
		$(DOCKER_CONTEXT)

build-01: $(DOCKER_CONTEXT) $(DOCKER_CONTEXT)/disdat
	@echo "---------- Building Disdat and its Python package dependencies"
	docker build \
		--build-arg IMAGE_LAYER=$(IMAGE_00_LAYER) \
		--build-arg BUILD_ROOT=$(IMAGE_BUILD_ROOT) \
		--build-arg DISDAT_SDIST=$(DISDAT_SDIST) \
		--file $(DOCKER_CONTEXT)/Dockerfiles/01-disdat-python.dockerfile  \
		--tag $(IMAGE_01_LAYER) \
		$(DOCKER_CONTEXT)

build-02: $(DOCKER_CONTEXT) $(DOCKER_CONTEXT)/config $(DOCKER_CONTEXT)/pipeline
	@echo "---------- Installing the user environment"
	docker build \
		--build-arg OS_NAME=$(OS_NAME) \
		--build-arg IMAGE_LAYER=$(IMAGE_01_LAYER) \
		--build-arg PIPELINE_ROOT=$(IMAGE_PIPELINE_ROOT) \
		--file $(DOCKER_CONTEXT)/Dockerfiles/02-user.dockerfile \
		--tag $(PIPELINE_IMAGE_NAME) \
		$(DOCKER_CONTEXT)

build-03: $(DOCKER_CONTEXT)
	@echo "---------- Using SageMaker entrypoint"
	docker build \
		--build-arg IMAGE_LAYER=$(PIPELINE_IMAGE_NAME) \
		--file $(DOCKER_CONTEXT)/Dockerfiles/03-sagemaker.dockerfile \
		--tag $(SAGEMAKER_TRAIN_IMAGE_NAME) \
		$(DOCKER_CONTEXT)

#
# Set up the Docker context in which we build the image
#

context: $(DOCKER_CONTEXT)

# Copy the dockerizer support files into the Docker context. For this copy,
# we DON'T want the directory name to be included in the copy.
$(DOCKER_CONTEXT): $(shell find $(DOCKER_CONTEXT_TEMPLATE) \! -name '*.pyc' -type f)
	@echo "----- Creating temporary Docker context in $@"
	@if [ ! -d $@ ]; then mkdir $@; fi
	rsync -av --delete --force $(DOCKER_CONTEXT_TEMPLATE)/ $@
	@touch $@

# Copy the user configuration files into the Docker context
$(DOCKER_CONTEXT)/config: $(DOCKER_CONTEXT) $(shell find $(CONFIG_ROOT) -type f)
	@echo "----- Copying configuration files from $(CONFIG_ROOT)"
	rsync -av --delete --force --quiet $(CONFIG_ROOT)/ $@
	@touch $@

# Copy Disdat into the Docker context
$(DOCKER_CONTEXT)/disdat: $(DOCKER_CONTEXT) $(shell find $(DISDAT_ROOT)/disdat -type f)
	@echo "----- Copying Disdat files from $(DISDAT_ROOT)"
	rsync -av --delete --force --quiet \
		$(DISDAT_ROOT)/disdat/infrastructure/dockerizer $(DISDAT_ROOT)/disdat/infrastructure $@
	@touch $@

# Copy the user pipeline package into the Docker context.
# We assume the user has a setup.py that can create an sdist.
$(DOCKER_CONTEXT)/pipeline: $(DOCKER_CONTEXT) $(shell find $(PIPELINE_ROOT) -not -path '*/\.*' -type f)
	@echo "----- Copying pipeline $(PIPELINE_ROOT)"
	cd $(PIPELINE_ROOT); \
	    python $(PIPELINE_ROOT)/setup.py sdist --dist-dir $@; cd -
	@touch $@


#
# Push the pipeline image to a registry
#

push:
ifeq ($(DOCKER_REGISTRY), )
	@echo "ERROR: DOCKER_REGISTRY variable not set"
	@exit 1
endif
ifneq ($(DOCKER_REGISTRY_IS_ECR), )
	@$(shell aws ecr get-login --no-include-email)
endif
	docker tag $(PIPELINE_IMAGE_NAME) $(DOCKER_REGISTRY)/$(PIPELINE_IMAGE_NAME)
	docker push $(DOCKER_REGISTRY)/$(PIPELINE_IMAGE_NAME)
