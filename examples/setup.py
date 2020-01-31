from setuptools import find_packages, setup

setup(
    name='disdat-example-pipelines',
    version=0.1,
    packages=find_packages(exclude=['config']),
    include_package_data=True,
    install_requires=[
        'luigi==2.8.9',
        'spacy',
        'pandas==0.24.2',
        'tensorflow==1.14.0',
    ],
)
