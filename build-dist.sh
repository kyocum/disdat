#!/usr/bin/env bash

echo "Building Disdat package for local installation or PyPi . . ."

# Bump version up -- Can use release or patch or major or minor
# bumpversion --dry-run --verbose release disdat/VERSION

# Now bump version for real
# and git commit -am "<version>"
# git tag <version>

# Remove the prior tar ball from the context.template
rm -rf  disdat/infrastructure/dockerizer/context.template/disdat-*.tar.gz
rm -rf  dist/disdat-*.tar.gz

# Create a new sdist
python setup.py sdist

# Copy over to the context.template.
cp dist/disdat-*.tar.gz disdat/infrastructure/dockerizer/context.template/.

# Create a new sdist that will have that tar.gz in the template
python setup.py sdist

# publish to test pypi
<<<<<<< HEAD
if true; then
    echo "Uploading to PYPI test and real"
    #twine upload --repository-url https://test.pypi.org/legacy/ dist/disdat-*.tar.gz
    # now do it for real
    #twine upload dist/disdat-*.tar.gz
=======
if false; then
    echo "Uploading to PYPI test and real"
    #twine upload --repository-url https://test.pypi.org/legacy/ dist/disdat-0.7.2rc0.tar.gz
    # now do it for real
    #twine upload dist/disdat-0.7.2rc0.tar.gz
>>>>>>> master
fi

echo "Finished"

