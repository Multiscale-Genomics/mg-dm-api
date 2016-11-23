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
- `<user_id>` - Identifier to uniquely locate the users files. Can be set to "common" if the files can be shared between users
- `<file_path>` - Location of the file in the file system
- `<file_type>` - File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
- `<data_type>` - The type of information in the file (RNA-seq, ChIP-seq, etc)
- `<source_id>` - List of IDs of files that were processed to generate this file
- `<meta_data>` - Dictionary object containing the extra data related to the generation of the file or describing the way it was processed

### Returns
`<unique_file_id>` - This is an id for that file within the system and can be used for tracing this file and where it is used and where it has come from

### example
```
from dmp import dmp
da = dmp()
unique_file_id = da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq')
```

If the is the processed result of 1 or more files then these can be specified using the file_id:

```
da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq', source_id=[1, 2])
```

Meta data about the file can also be included to provide extra information about the file, origins or how it was generated:

```
da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq', meta={'downloaded_from' : 'http://www.', })
```

## get_file_by_id
Get the file based on the `<unique_file_id>`

### Options
- `<user_id>` - Identifier to uniquely locate the users files. Can be set to "common" if the files can be shared between users
- `<unique_file_id>` - The id returned by the `<set_file>` function.

### Returns
Dictionary object containing:
- `<user_id>` - Identifier to uniquely locate the users files. Can be set to "common" if the files can be shared between users
- `<file_path>` - Location of the file in the file system
- `<file_type>` - File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
- `<data_type>` - The type of information in the file (RNA-seq, ChIP-seq, etc)
- `<source_id>` - List of IDs of files that were processed to generate this file
- `<meta_data>` - Dictionary object containing the extra data related to the generation of the file or describing the way it was processed
- `<creation_time>` - Time at which the file was loaded into the system

### example
```
from dmp import dmp
da = dmp()
da.get_file_by_id(<unique_file_id>)
```

## get_files_user
Get a list of the file dictionary objects given a `user_id`

### Options
- `<user_id>` - Identifier to uniquely locate the users files. Can be set to "common" if the files can be shared between users

### Returns
List of dictionary objects. Each one containing the following:
- `<user_id>` - Identifier to uniquely locate the users files. Can be set to "common" if the files can be shared between users
- `<file_path>` - Location of the file in the file system
- `<file_type>` - File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
- `<data_type>` - The type of information in the file (RNA-seq, ChIP-seq, etc)
- `<source_id>` - List of IDs of files that were processed to generate this file
- `<meta_data>` - Dictionary object containing the extra data related to the generation of the file or describing the way it was processed
- `<creation_time>` - Time at which the file was loaded into the system

### example
```
from dmp import dmp
da = dmp()
da.get_files_by_user(<user_id>)
```

## get_files_by_file_type
Get a list of the file dictionary objects given a `user_id` and `file_type`

### Options
- `<user_id>` - Identifier to uniquely locate the users files. Can be set to "common" if the files can be shared between users
- `<file_type>` - File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)

### Returns
List of dictionary objects. Each one containing the following:
- `<user_id>` - Identifier to uniquely locate the users files. Can be set to "common" if the files can be shared between users
- `<file_path>` - Location of the file in the file system
- `<file_type>` - File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
- `<data_type>` - The type of information in the file (RNA-seq, ChIP-seq, etc)
- `<source_id>` - List of IDs of files that were processed to generate this file
- `<meta_data>` - Dictionary object containing the extra data related to the generation of the file or describing the way it was processed
- `<creation_time>` - Time at which the file was loaded into the system

### example
```
from dmp import dmp
da = dmp()
da.get_files_by_file_type(<user_id>, <file_type>)
```


## get_files_by_data_type
Get a list of the file dictionary objects given a `user_id` and `data_type`

### Options
- `<user_id>` - Identifier to uniquely locate the users files. Can be set to "common" if the files can be shared between users
- `<data_type>` - The type of information in the file (RNA-seq, ChIP-seq, etc)

### Returns
List of dictionary objects. Each one containing the following:
- `<user_id>` - Identifier to uniquely locate the users files. Can be set to "common" if the files can be shared between users
- `<file_path>` - Location of the file in the file system
- `<file_type>` - File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
- `<data_type>` - The type of information in the file (RNA-seq, ChIP-seq, etc)
- `<source_id>` - List of IDs of files that were processed to generate this file
- `<meta_data>` - Dictionary object containing the extra data related to the generation of the file or describing the way it was processed
- `<creation_time>` - Time at which the file was loaded into the system

### example
```
from dmp import dmp
da = dmp()
da.get_files_by_data_type(<user_id>, <data_type>)
```


## get_file_history
For a given `<unique_file_id>` retrieve the list of files that were used in its generation.

### Options
- `<unique_file_id>` - ID of the file. This is the value returned when a file is loaded into the DMP or is the `_id` for a given file when the files have bee retrieved.

### Returns
Dictionary of lists. For the queried `<unique_file_id>` this is the key for a list of the parent objects, each recursively containing a list of the parent `<unique_file_id>`s

### example
```
from dmp import dmp
da = dmp()
history = da.get_file_history("58357157d9422a2b1700a0d5")
print history
```
Output:
```
{'58357157d9422a2b1700a0d5': [{u'58357017d9422a2b4292d878': []}]}
```
These IDs can then be requested to ruturn the meta data and locations with the `get_file_by_id` method.
