#!/usr/bin/env bash

echo "Building Disdat package for local installation or PyPi . . ."

# Use git to tag the release with the semver you wish
# git tag <version>

# Create a new sdist
python setup.py sdist

# publish to test pypi
if false; then
    echo "Uploading to PYPI test and real"
    #twine upload --repository-url https://test.pypi.org/legacy/ dist/disdat-*.tar.gz
    # Test: pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple disdat
    # now do it for real
    twine upload dist/disdat-*.tar.gz
fi

echo "Finished"

