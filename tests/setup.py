from setuptools import find_packages, setup

setup(
    name='disdat-test-pipelines',
    version=0.1,
    packages=find_packages(exclude=['config']),
    include_package_data=True,
    install_requires=[
        'pandas'
    ],
)
