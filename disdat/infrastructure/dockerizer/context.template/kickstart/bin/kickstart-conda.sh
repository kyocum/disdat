#!/bin/bash
#
# Kickstart a basic Miniconda virtual environment. You can the use this
# basic environment to kickstart a more built-up Python virtual environment.

SH_FILE=$(basename $0)
SH_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source $SH_DIR/../etc/common.rc

if [ x$CONF_DIR == 'x' ]; then
	CONF_DIR=$SH_DIR/../etc
fi
source $CONF_DIR/kickstart.conf

USAGE="Usage: $SH_FILE [-h] [-d] [-v] [-c CONDA_VERSION] [-m CONDA_ROOT] VIRTUAL_ENV"

conda_root=$CONDA_ROOT_DEFAULT
conda_version=$CONDA_VERSION_DEFAULT

function usage() {
cat << EOF
Install Miniconda with Intel MKL and a corresponding Python 2.7 interpreter.

$USAGE

Positional arguments:
    VIRTUAL_ENV : The destination virtual environment path

Options:
    -h : Get help
    -d : Set debug mode (echoes commands)
    -c CONDA_VERSION : Specify the Miniconda version (default $CONDA_VERSION_DEFAULT);
         set to '' to use the most recent version
    -m CONDA_ROOT : Specify the Miniconda installation root
         (default $CONDA_ROOT_DEFAULT)
    -v : Verbose : Warn if we are reusing an existing virtual environment
EOF
}

while getopts "hdc:m:v" opt; do
	case "$opt" in
	h)	usage
		exit 0
		;;
	d)	set -x
		;;
	c)	conda_version=$OPTARG
		;;
	m)	conda_root=$OPTARG
		;;
	v)	verbose=yes
		;;
	*)
		echo $USAGE
		exit 1
	esac
done

shift $((OPTIND-1))

virtual_env=${1}
if [ x$virtual_env == 'x' ]; then
	error "Could not find VIRTUAL_ENV argument"
	echo $USAGE
	exit 1
fi

echo "Using Miniconda installation root $conda_root"
echo "Using Python virtual environment $virtual_env"

if [ ! -d $conda_root ]; then
	# Need to create a virtual environment to hold the conda installer.
	if [ x$(which virtualenv) == 'x' ]; then
		error "Failed to find virtualenv command"
		exit 1
	fi

	# Miniconda makes a mess out of the PATH. Thanks Continuum.
	old_path=$PATH
	virtualenv $conda_root
	source $conda_root/bin/activate

	# Whatever happens now, make sure we deactivate the virtual environment
	function atexit_deactivate {
		deactivate
		export PATH=$old_path
	}
	trap atexit_deactivate EXIT

	# Install security packages to prevent pip warnings about SSLContexts.
	pip install --upgrade ndg-httpsclient pyasn1 pyOpenSSL

	# auxlib is required by conda. mock is required to support kickstarting
	# from setup.py instead of requirements.txt
	pip install auxlib mock
	if [ x$conda_version != x ]; then
		pip install conda==$conda_version conda-env
	else
		pip install conda conda-env
	fi

	if [ x$(which conda) == 'x' ]; then
		error "Failed to find conda command"
		exit 1
	fi
fi

if [ ! -d $virtual_env ]; then
	if ! $conda_root/bin/conda create -p $virtual_env --yes python=2; then
		error "Failed to create a Miniconda environment in $virtual_env"
		exit 1
	fi

	# More conda PATH nonsense - our deactivate script cleans up the PATH
	# completely, leaving no trace of conda cruft.
	mv $virtual_env/bin/deactivate $virtual_env/bin/deactivate-original
	# You'd better believe it - we need a lot of escaping to make this
	# work
	conda_root_escaped=$(echo $conda_root | sed 's/\//\\\\\\\\\\\\\\\//g')
	sed "s/%%CONDA_ROOT%%/$conda_root_escaped/" $CONF_DIR/deactivate.skel > $virtual_env/bin/deactivate
	chmod +x $virtual_env/bin/deactivate
	# Flag this virtual environment so we remember that we have a custom
	# deactivate script
	touch $virtual_env/$CONDA_FLAG_FILE
	success "Successfully installed the Miniconda environment in $virtual_env"
	success "'source $virtual_env/bin/activate $virtual_env' to start using it"
elif [ -f $virtual_env/$CONDA_FLAG_FILE ]; then
	if [ x$verbose != 'x' ]; then
		warning "Already installed a Miniconda environment in $virtual_env"
	else
		echo "Reusing an existing Miniconda environment in $virtual_env"
	fi
	echo "'source $virtual_env/bin/activate $virtual_env' to start using it"
else
	error "Failed to create a Miniconda environment: $virtual_env exists and is in use"
	exit 1
fi
