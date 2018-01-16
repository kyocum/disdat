#!/usr/bin/env bash

echo "Building Disdat executable dsdt . . ."


if [ -z ${VIRTUAL_ENV+x} ]; then
    echo "No current virtual environment";
else
    echo "Deactivating current venv $VIRTUAL_ENV";
    oldpath=$PATH
    export PATH=`echo $PATH | cut -d : -f 2-`
    oldve=$VIRTUAL_ENV
    unset VIRTUAL_ENV
    oldps1=$PS1
    unset PS1
fi

# Assume they have virtualenvwrapper installed in root environment

source `which virtualenvwrapper.sh`

oldwoh=$WORKON_HOME

export WORKON_HOME=~/.virtual_envs

mkvirtualenv disdat-dist

pip install -e .

pip install pyinstaller

# remove all the .pyc files so we don't copy them into the binary folder
rm `find . -name '*.pyc'`

#pyinstaller --clean --onefile --name dsdt disdat/dsdt.py
pyinstaller  --onefile --name dsdt dsdt-mod.spec

if false; then
    # At one point, pyinstaller was failing.   No longer apears to be the case, but holding on to vistigial code.
    boto_file=${WORKON_HOME}/disdat-dist/lib/python2.7/site-packages/PyInstaller/hooks/hook-botocore.py
    sed -i -e 's/from PyInstaller.utils.hooks import collect_data_files/from PyInstaller.utils.hooks import collect_data_files, is_module_satisfies/' $boto_file
    sed -i -e 's/from PyInstaller.compat import is_py2, is_module_satisfies/from PyInstaller.compat import is_py2/' $boto_file
    pyinstaller --onefile  --name dsdt dsdt-mod.spec
fi


# Manual re-activate
if [ -z ${oldve+x} ]; then
    unset VIRTUAL_ENV
else
    export PATH=$oldpath
    export VIRTUAL_ENV=$oldve
    export PS1=$oldps1
fi

rmvirtualenv disdat-dist

export WORKON_HOME=$oldwoh

echo "Finished"

