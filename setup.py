"""
.. See the NOTICE file distributed with this work for additional information
   regarding copyright ownership.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

from setuptools import setup, find_packages

setup(
    name='dmp',
    version='0.0.1',
    description='MuG DMP API',

    url='http://www.multiscalegenomics.eu',
    download_url='https://github.com/Multiscale-Genomics/mg-dm-api',

    author='Mark McDowall',
    author_email='mcdowall@ebi.ac.uk',

    license='Apache 2.0',

    packages=find_packages(),
    package_data={
        'dm_test_data': [
            'sample.bb', 'sample.bw', 'sample_3D_models.hdf5', 'sample_adjacency.hdf5'
        ]
    },

    install_requires=[
        'pymongo>=3.3', 'mongomock>=3.7', 'configparser', 'numpy', 'h5py',
        'pytest'
    ],

    tests_require=[
        'pytest',
    ],
)
