#!/bin/bash
#
# Kickstart a Python virtual environment. Tries to deal with the differences
# between installing pip-managed and conda-managed environments.

SH_FILE=$(basename $0)
SH_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source $SH_DIR/../etc/common.rc

if [ x$CONF_DIR == 'x' ]; then
	CONF_DIR=$SH_DIR/../etc
fi
source $CONF_DIR/kickstart.conf

USAGE="Usage: $SH_FILE [-h] [-d] [-a] [-f] [-n] [-v] [-c CONDA_VERSION] [-m CONDA_ROOT] <VIRTUAL_ENV> <REQUIREMENTS_FILE>"

conda_root=$CONDA_ROOT_DEFAULT
conda_version=$CONDA_VERSION_DEFAULT
use_conda=''

force_pip=''

no_cache=''

function usage() {
cat << EOF
Install dependencies in a Python virtual environment.

$USAGE

Positional arguments:
    VIRTUAL_ENV : The destination virtual environment path
    REQUIREMENTS_FILE : An optional pip-style requirements file

Options:
    -h : Get help
    -d : Set debug mode (echoes commands)
    -a : Use Miniconda with Intel MKL instead of pip install
    -b : Build binaries for non-pure-Python dependencies from source (i.e.,
         do not use manylinux wheels)
    -c CONDA_VERSION : Specify the Miniconda version (default $CONDA_VERSION_DEFAULT);
         set to '' to use the most recent version
    -m CONDA_ROOT : Specify the Miniconda installation root
         (default $CONDA_ROOT_DEFAULT)
    -f : Force pip install even if we detect Miniconda with Intel MKL
    -n : No cache : Disable pip package/wheel caches, and flush Miniconda
         cache after kickstart
    -v : Verbose : Warn if we are reusing an existing virtual environment

WARNING: If your requirements file contains a relative pathname (for
example, when supplying an 'editable' package such as '-e .', the installer
will do the wrong thing if your working directory is not the directory that
contains the requirements file.
EOF
}

while getopts "hdabc:fm:nv" opt; do
	case "$opt" in
	h)	usage
		exit 0
		;;
	d)	set -x
		debug=yes
		;;
	a)	use_conda=yes
		;;
	b)	no_manylinux=yes
		;;
	c)	conda_version=$OPTARG
		;;
	f)	force_pip=yes
		;;
	m)	conda_root=$OPTARG
		use_conda=yes
		;;
	n)	no_cache='--no-cache-dir'
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

has_requirements=yes
requirements=${2}
if [ x$requirements == 'x' ]; then
     has_requirements=''
elif [ ! -e $requirements ]; then
	error "Could not find requirements file $requirements"
	exit 1
elif [ ! -f $requirements ]; then
	error "Got invalid requirements file $requirements: Not a regular file"
	exit 1
fi

# If the 'requirements' file is actually a source distribution, extract the
# setup.py file.
if [ x$has_requirements != 'x' ]; then
    package_name_maybe=$(basename $requirements .tar.gz)
    setup_py_dir=
    if [ $(basename $requirements) == ${package_name_maybe}.tar.gz ]; then
        # Sorry - no cleanup if the script dies...
        setup_py_dir=$(mktemp -d)
        if ! tar xvzf $requirements -C $setup_py_dir $package_name_maybe/setup.py; then
            error "Got invalid Python sdist $requirements"
            exit 1
        fi
        requirements=$setup_py_dir/$package_name_maybe/setup.py
    fi
fi

if [ x$use_conda != 'x' -a x$force_pip != 'x' ]; then
	error "Got conflicting requests: Use conda or force pip?"
	exit 1
fi

if [ x$no_cache != 'x' -a x$verbose != 'x' ]; then
	warning "pip/conda caches disabled"
fi

if [ \( x$use_conda != 'x' -o -x $virtual_env/bin/conda \) -a x$force_pip == 'x' ]; then
	if [ x$conda_version != x ]; then
		conda_version_flag="-c $conda_version"
	else
		conda_version_flag=
	fi
	if ! $SH_DIR/kickstart-conda.sh $conda_version_flag -m $conda_root $virtual_env; then
		error "Failed to install basic Miniconda virtual envionment"
		exit 1
	fi

	source $virtual_env/bin/activate $virtual_env
	# Whatever happens now, make sure we deactivate the virtual environment
	# and remove the temp file.
	non_conda_requirements=$(mktemp)
	function atexit_deactivate {
		if [ x$debug == 'x' ]; then
			rm -f $non_conda_requirements
		fi
		source deactivate
	}
	trap atexit_deactivate EXIT

	install_pip
	# mock is required by select_conda_packages.py
	run pip install --quiet mock

	if [ x$no_manylinux != 'x' ]; then
		# Prevent pip from installing generic 'manylinux' wheels.
		run cp -p $SH_DIR/../etc/_manylinux.py $virtual_env/lib/python2.7/site-packages
	fi

	# Install whatever Continuum provides
	conda_packages=$($SH_DIR/select_conda_packages.py $requirements 2> $non_conda_requirements)
	if ! conda install --yes $conda_packages mkl mkl-service; then
		error "Failed to install Python requirements"
		exit 1
	fi
	# Install whatever Continuum doesn't provide. If the input was a
	# setup.py file, we have to use the recycled output from
	# select_conda_packages.py.
	if [ x$has_requirements != 'x' ]; then
        if [ $(basename $requirements) == "setup.py" ]; then
            if [ x$(wc -w $non_conda_requirements | awk '{print $1}') != 'x0' ]; then
                if ! pip --disable-pip-version-check $no_cache install -r $non_conda_requirements; then
                    error "Failed to install pip-only Python requirements"
                    exit 1
                fi
            fi
        else
            if ! pip --disable-pip-version-check $no_cache install -r $requirements; then
                error "Failed to install pip-only Python requirements"
                exit 1
            fi
        fi
    fi

	if [ x$no_cache != 'x' ]; then
		if ! conda clean -a -y; then
			warning "Failed to flush Miniconda cache"
		fi
	fi

	use_conda=yes
else
	echo "Using Python virtual environment $virtual_env"

	if [ x$(which virtualenv) == 'x' ]; then
		error "Failed to find virtualenv command"
		exit 1
	fi

	VIRTUALENV_VERSION=15.1.0
	if [ $(virtualenv --version) != $VIRTUALENV_VERSION ]; then
		warning "Expected virtualenv $VIRTUALENV_VERSION, got $(virtualenv --version)"
	fi

	if [ -d $virtual_env -a x$force_pip == 'x' ]; then
		if [ x$verbose != 'x' ]; then
			warning "Found an existing directory $(virtual_env); not running virtualenv"
		fi
	else
		virtualenv $virtual_env
	fi
	source $virtual_env/bin/activate

	# If we succeeded, the activation script will set VIRTUAL_ENV
	if [ x$VIRTUAL_ENV == 'x' ]; then
		error "Failed to activate Python virtual environment in $virtual_env"
		exit 1
	fi

	# Whatever happens now, make sure we deactivate the virtual environment
	old_cwd=$PWD
	requirements_from_setup=$(mktemp)
	function atexit_deactivate {
		if [ x$debug == 'x' ]; then
			rm -f $requirements_from_setup
		fi
		cd $old_cwd
		deactivate
	}
	trap atexit_deactivate EXIT

	install_pip
	# mock is required by find_packages_from_setup.py
	run pip install --quiet mock

	if [ x$no_manylinux != 'x' ]; then
		# Prevent pip from installing generic 'manylinux' wheels.
		run cp -p $SH_DIR/../etc/_manylinux.py $virtual_env/lib/python2.7/site-packages
	fi

	# The authors of SciPy feel that dependency checking is (a) for the
	# weak, or (b) for anyone that actually wants to get work done instead
	# of dorking around for hours with arcane 'pip install' misfires, so
	# it must be installed outside of 'pip install -r requirements.txt'.
    if [ x$has_requirements != 'x' ]; then
        if [ $(basename $requirements) == "setup.py" ]; then
            $SH_DIR/find_packages_from_setup.py $requirements > $requirements_from_setup
            requirements=$requirements_from_setup
        fi
        numpy=$(grep ^numpy $requirements)
        if [ -n "$numpy" ] && ! pip --disable-pip-version-check $no_cache install $numpy; then
            error "Failed to install $numpy"
            exit 1
        fi
        scipy=$(grep ^scipy $requirements)
        if [ -n "$scipy" ] && ! pip --disable-pip-version-check $no_cache install $scipy; then
            error "Failed to install $scipy"
            exit 1
        fi
        if ! pip --disable-pip-version-check $no_cache install -r $requirements; then
            error "Failed to install Python requirements"
            exit 1
        fi
    fi
fi

success "Successfully installed the Python virtual environment"
if [ x$use_conda != 'x' ]; then
	success "'source $virtual_env/bin/activate $virtual_env' to start using it"
else
	success "'source $virtual_env/bin/activate' to start using it"
fi

if [ x$setup_py_dir != x ]; then
	rm -r $setup_py_dir
fi
