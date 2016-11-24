# mg-dm-api

API for the management of file locations and users within the MuG VRE.

# Requirements
- Mongo DB 3.2
- Python 2.7.10+
- Python Modules:
  - pymongo

# Configuration file
Requires a file with the name `dmp.cnf` withthe following parameters to define the MongoDB server:
```
[dmp]
host = localhost
port = 27017
user = 
pass = 
db = dmp
```

Full documentation can be found on [ReadTheDocs](http://mg-dm-api.readthedocs.io)
