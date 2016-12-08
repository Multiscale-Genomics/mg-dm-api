# mg-dm-api

[![Documentation Status](https://readthedocs.org/projects/mg-dm-api/badge/?version=latest)](http://mg-dm-api.readthedocs.org/en/latest/)

API for the management of file locations and users within the MuG VRE.

Full documentation can be found on [ReadTheDocs](http://mg-dm-api.readthedocs.io)

# Requirements
- Mongo DB 3.2
- Python 2.7.10+
- Python Modules:
  - pymongo
  - monogomock

# Installation
Cloneing from GitHub:
```
git clone https://github.com/Multiscale-Genomics/mg-dm-api.git
```
To get this to be picked up by pip if part of a webserver then:
```
pip install --editable .
```
This should install the required packages listed in the `setup.py` script.


Installation via pip:
```
pip install git+https://github.com/Multiscale-Genomics/mg-dm-api.git
```

# Configuration file
Requires a file with the name `dmp.cnf` with the following parameters to define the MongoDB server:
```
[dmp]
host = localhost
port = 27017
user = 
pass = 
db = dmp
ftp_root = ftp://ftp.<url_root>
```
