Functions
=========

``set_file``
------------

Adds a file to the data management API.

Options
^^^^^^^

- `<user_id>` - Identifier to uniquely locate the users files. Can be set to "common" if the files can be shared between users
- ``<file_path>`` - Location of the file in the file system
- ``<file_type>`` - File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
- ``<data_type>`` - The type of information in the file (RNA-seq, ChIP-seq, etc)
- ``<source_id>`` - List of IDs of files that were processed to generate this file
- ``<meta_data>`` - Dictionary object containing the extra data related to the generation of the file or describing the way it was processed

Returns
^^^^^^^
``<unique_file_id>`` - This is an id for that file within the system and can be used for tracing this file and where it is used and where it has come from

Example
^^^^^^^

.. code-block:: python
    from dmp import dmp
    da = dmp()
    unique_file_id = da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq')

If the is the processed result of 1 or more files then these can be specified using the file_id:

.. code-block:: python
    da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq', source_id=[1, 2])

Meta data about the file can also be included to provide extra information about the file, origins or how it was generated:

.. code-block:: python
    da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq', meta={'downloaded_from' : 'http://www.', })

