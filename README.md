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

# Functions:
## set_file
Adds a file to the data management API.

### Options
- <user_id> - Identifier to uniquely locate the users files. Can be set to "common" if the files can be shared between users
- <file_path> - Location of the file in the file system
- <file_type> - File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
- <data_type> - The type of information in the file (RNA-seq, ChIP-seq, etc)
- <source_id> - List of IDs of files that were processed to generate this file
- <meta_data> - Dictionary object containing the extra data related to the generation of the file or describing the way it was processed

### example
```
from dmp import dmp
da = dmp()
da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq')
```

If the is the processed result of 1 or more files then these can be specified using the file_id:

```
da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq', source_id=[1, 2])
```

Meta data about the file can also be included to provide extra information about the file, origins or how it was generated:

```
da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq', meta={'downloaded_from' : 'http://www.', })
```
