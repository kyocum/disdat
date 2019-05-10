from setuptools import find_packages, setup

setup(
    name='disdat-example-pipelines',
    version=0.1,
    packages=find_packages(exclude=['config']),
    install_requires=[
        'luigi',
        'pandas==0.20.3',
        'tensorflow',
    ],
)
