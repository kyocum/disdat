#!/bin/bash
#
# Kickstart Disdat

SH_FILE=$(basename $0)
SH_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source $SH_DIR/../etc/common.rc

if [ x$CONF_DIR == 'x' ]; then
	CONF_DIR=$SH_DIR/../etc
fi
source $CONF_DIR/kickstart.conf

USAGE="Usage: $SH_FILE [-h] [-d] VIRTUAL_ENV PACKAGE_ROOT"

function usage() {
cat << EOF
Install a Python package from its source tree into a Python virtual
environment. To simplify matters, you must have previously kickstarted the
target environment, otherwise this script would have to provide all manner
of pass-through arguments.

$USAGE

Positional arguments:
    VIRTUAL_ENV : The destination virtual environment path
    PACKAGE_ROOT : The root of the package source

Options:
    -h : Get help
    -d : Set debug mode (echoes commands)
EOF
}

while getopts "hd" opt; do
	case "$opt" in
	h)	usage
		exit 0
		;;
	d)	set -x
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

package_root=${2}
if [ x$package_root == 'x' ]; then
	error "Could not find PACKAGE_ROOT argument"
	echo $USAGE
	exit 1
fi

echo "Using Python virtual environment $virtual_env"
echo "Using package root $package_root"

# Whatever happens now, make sure we deactivate the virtual environment
# and remove the temp file.
old_cwd=$PWD
if [ -f $virtual_env/bin/activate ]; then
	if [ -x $virtual_env/bin/conda ]; then
		source $virtual_env/bin/activate $virtual_env
		function atexit_deactivate {
			source deactivate
			cd $old_cwd
		}
		trap atexit_deactivate EXIT
		use_conda=yes
	else
		source $virtual_env/bin/activate
		function atexit_deactivate {
			deactivate
			cd $old_cwd
		}
		trap atexit_deactivate EXIT
	fi
else
	error "$virtual_env is not a valid Python virtual environment"
	exit 1
fi

echo "Using Python interpreter $(which python)"
if [ -d $package_root ]; then
	echo "cd $package_root; python setup.py install"
	cd $package_root
	run python setup.py install
else
    echo "pip install $package_root"
	pip install $package_root
fi
success "Successfully installed $(basename $package_root) in $virtual_env"
if [ x$use_conda != 'x' ]; then
	success "'source $virtual_env/bin/activate $virtual_env' to start using it"
else
	success "'source $virtual_env/bin/activate' to start using it"
fi
