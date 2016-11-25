import sys
import codecs

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

import dmp

install_requires = [
    'pymongo>=3.3'
]

# bdist_wheel
extras_require = {
    # http://wheel.readthedocs.io/en/latest/#defining-conditional-dependencies
    ':python_version == "2.7"'
}

setup(
    name='dmp',
    version=dmp.__version__,
    description='MuG DMP API',
    url='http://www.multiscalegenomics.eu',
    download_url='https://github.com/Multiscale-Genomics/mg-dm-api',
    author=dmp.__author__,
    author_email='mcdowall@ebi.ac.uk',
    license=dmp.__licence__,
    packages=find_packages(),
)
