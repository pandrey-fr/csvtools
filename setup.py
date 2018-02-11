# coding: utf-8

"""Setup for the installation of the 'csvtools' package.

Installation in a virtual environment is strongly advised.
"""

import setuptools

import csvtools


setuptools.setup(
    name='csvtools',
    version=csvtools.__version__,
    packages=setuptools.find_packages(),
    include_package_data=True,
    author='Paul Andrey',
    description='CSVTools - tools for csv files manipulation',
    long_description=open('README.md').read(),
    url='https://github.com/pandrey-fr/csvtools/',
    install_requires=[
        'pandas >= 0.20.1',
    ],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent"
    ]
)
