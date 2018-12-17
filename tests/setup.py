from setuptools import find_packages, setup

setup(
    name='pipelines',
    version=0.1,
    packages=find_packages(exclude=['config']),
    install_requires=[
        'luigi',
        'pandas'
    ],
)
