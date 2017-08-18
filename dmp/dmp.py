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

from __future__ import print_function, unicode_literals

import datetime
import os
import random
import sys
import configparser

import pymongo
from pymongo import MongoClient, ReadPreference
from bson.objectid import ObjectId


class dmp(object): # pylint: disable=invalid-name
    """
    API for management of files within the VRE
    """

    def __init__(self, cnf_loc='', test=False):
        """
        Initialise the module and setup parameters
        """

        config = configparser.RawConfigParser()
        config.read(cnf_loc)

        self.ftp_root = "ftp://test.test_url.org/"

        if test is True:
            import mongomock
            self.client = mongomock.MongoClient()
            self.db = self.client["dmp"] # pylint: disable=invalid-name
            self._test_loading_dataset()
        else:
            host = config.get("dmp", "host")
            port = config.getint("dmp", "port")
            user = config.get("dmp", "user")
            password = config.get("dmp", "pass")
            dmp_db = config.get("dmp", "db")
            self.ftp_root = config.get("dmp", "ftp_root")

            try:
                self.client = MongoClient(
                    host, port,
                    read_preference=ReadPreference.SECONDARY_PREFERRED
                )
                self.client.admin.authenticate(user, password)
                self.db = self.client[dmp_db]
            except:
                error = sys.exc_info()[0]
                print("Error: %s" % error)
                sys.exit(1)

        self.entries = self.db.entries
        self.db.entries.create_index(
            [('user_id', pymongo.ASCENDING)],
            unique=False, background=True)
        self.db.entries.create_index(
            [('user_id', pymongo.ASCENDING), ('file_type', pymongo.ASCENDING)],
            unique=False, background=True)
        self.db.entries.create_index(
            [('user_id', pymongo.ASCENDING), ('data_type', pymongo.ASCENDING)],
            unique=False, background=True)
        self.db.entries.create_index(
            [('user_id', pymongo.ASCENDING), ('taxon_id', pymongo.ASCENDING)],
            unique=False, background=True)


    def _test_loading_dataset(self):
        users = ["adam", "ben", "chris", "denis", "eric"]
        file_types = [
            "fastq", "fa", "fasta", "bam", "bed", "bb", "hdf5", "tsv", "gz",
            "tbi", "wig", "bw", "pdb", "prmtop", "trj", "dcd"
        ]
        data_types = ['RNA-seq', 'MNase-Seq', 'ChIP-seq', 'WGBS', 'HiC']
        compressed = [None, 'gzip', 'zip']

        resource_path = os.path.dirname(__file__)
        file_id = self.set_file(
            "rao", os.path.join(resource_path, 'rao2014.hdf5'), "hdf5", "HiC", 9606,
            meta_data={'assembly' : 'GCA_0123456789'}
        )

        file_id = self.set_file(
            "test", os.path.join(resource_path, '/tmp/sample.bb'),
            "bb", "RNA-seq", 9606,
            meta_data={'assembly' : 'GCA_0123456789'}
        )

        file_id = self.set_file(
            "test", '/tmp/sample_coords.hdf5',
            "hdf5", "HiC", 9606,
            meta_data={'assembly' : 'GCA_0123456789'}
        )

        file_id = self.set_file(
            "test", '/tmp/sample_adjacency.hdf5',
            "hdf5", "HiC", 9606,
            meta_data={'assembly' : 'GCA_0123456789'}
        )

        for user in users:
            data_type = 'RNA-seq'
            file_handle = '/tmp/test/' + data_type + '/test_rna-seq.fastq'
            file_type = "fastq"
            zipped = None
            file_id = self.set_file(
                user, file_handle, file_type, data_type, 9606, None, [],
                meta_data={'assembly' : 'GCA_0123456789'})
            file_handle = '/tmp/test/' + data_type + '/test_rna-seq.bam'
            self.set_file(
                user, file_handle, 'bam', data_type, 9606, None, [file_id],
                meta_data={'assembly' : 'GCA_0123456789'})

        for i in range(10):
            user = random.choice(users)
            file_type = random.choice(file_types)
            data_type = random.choice(data_types)
            zipped = random.choice(compressed)
            file_handle = '/tmp/test/' + data_type + '/test_' + str(i) + '.' + file_type
            file_id = self.set_file(
                user, file_handle, file_type, data_type, 9606, zipped,
                meta_data={'assembly' : 'GCA_0123456789'})

            if data_type == 'RNA-seq' and file_type == 'fastq' and random.choice([0, 1]) == 1:
                file_handle = '/tmp/test/' + data_type + '/test_' + str(i) + '.bam'
                self.set_file(
                    user, file_handle, 'bam', data_type, 9606, None, [file_id],
                    meta_data={'assembly' : 'GCA_0123456789'})


    def get_file_by_id(self, user_id, file_id):
        """
        Returns files data based on the unique_id for a given file

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
        file_id : str
            Location of the file in the file system

        Returns
        -------
        dict
            file_path : str
                Location of the file in the file system
            file_type : str
                File format ("fastq", "fasta", "bam", "bed", "bb", "hdf5",
                "tsv", "gz", "tbi", "wig", "bw", "pdb", "gem", "bt2", "amb",
                "ann", "bwt", "pac", "sa", "tif", 'lif', "prmtop", "trj", "dcd")
            data_type : str
                The type of information in the file (RNA-seq, ChIP-seq, etc)
            taxon_id : int
                Taxon ID that the species that the file has been derived from
            compressed : str
                Type of compression (None, gzip, zip)
            source_id : list
                List of IDs of files that were processed to generate this file
            meta_data : dict
                Dictionary object containing the extra data related to the
                generation of the file or describing the way it was processed
            creation_time : list
                Time at which the file was loaded into the system

        Example
        -------
        .. code-block:: python
           :linenos:

           from dmp import dmp
           da = dmp()
           da.get_file_by_id(<unique_file_id>)
        """
        entries = self.db.entries
        file_obj = entries.find_one({'_id': ObjectId(file_id), 'user_id': user_id})
        file_obj["_id"] = str(file_obj["_id"])
        file_obj["creation_time"] = str(file_obj["creation_time"])
        return file_obj


    def get_files_by_user(self, user_id, rest=False):
        """
        Get a list of the file dictionary objects given a `user_id`

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users

        Returns
        -------
        list
            List of dict objects for each file that has been loaded by a user.

        Example
        -------
        .. code-block:: python
           :linenos:

           from dmp import dmp
           da = dmp()
           da.get_files_by_user(<user_id>)
        """
        entries = self.db.entries
        files = []

        if rest is True:
            results = entries.find(
                {"user_id" : user_id},
                {
                    "file_type" : 1, "data_type" : 1, "taxon_id" : 1,
                    "source_id" : 1, "meta_data" : 1, "creation_time" : 1
                }
            )
        else:
            results = entries.find(
                {"user_id" : user_id},
                {
                    "file_path" : 1, "file_type" : 1, "data_type" : 1, "taxon_id" : 1,
                    "source_id" : 1, "meta_data" : 1, "creation_time" : 1
                }
            )
        for entry in results:
            entry["_id"] = str(entry["_id"])
            entry["creation_time"] = str(entry["creation_time"])
            files.append(entry)
        return files


    def get_files_by_file_type(self, user_id, file_type, rest=False):
        """
        Get a list of the file dictionary objects given a `user_id` and
        `file_type`

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
        file_type : str
            File format ("fastq", "fasta", "bam", "bed", "bb", "hdf5", "tsv",
            "gz", "tbi", "wig", "bw", "pdb", "gem", "bt2", "amb", "ann", "bwt",
            "pac", "sa", "tif", 'lif', "prmtop", "trj", "dcd")

        Returns
        -------
        dict
            file_path : str
                Location of the file in the file system
            file_type : str
                File format ("fastq", "fasta", "bam", "bed", "bb", "hdf5",
                "tsv", "gz", "tbi", "wig", "bw", "pdb", "gem", "bt2", "amb",
                "ann", "bwt", "pac", "sa", "tif", 'lif', "trj", "dcd")
            data_type : str
                The type of information in the file (RNA-seq, ChIP-seq, etc)
            taxon_id : int
                Taxon ID that the species that the file has been derived from
            compressed : str
                Type of compression (None, gzip, zip)
            source_id : list
                List of IDs of files that were processed to generate this file
            meta_data : dict
                Dictionary object containing the extra data related to the
                generation of the file or describing the way it was processed
            creation_time : list
                Time at which the file was loaded into the system

        Example
        -------
        .. code-block:: python
           :linenos:

           from dmp import dmp
           da = dmp()
           da.get_files_by_file_type(<user_id>, <file_type>)
        """
        entries = self.db.entries
        files = []

        if rest is True:
            results = entries.find(
                {"user_id" : user_id, "file_type" : file_type},
                {
                    "file_type" : 1, "data_type" : 1, "taxon_id" : 1,
                    "source_id" : 1, "meta_data" : 1, "creation_time" : 1
                }
            )
        else:
            results = entries.find(
                {"user_id" : user_id, "file_type" : file_type},
                {
                    "file_path" : 1, "file_type" : 1, "data_type" : 1, "taxon_id" : 1,
                    "source_id" : 1, "meta_data" : 1, "creation_time" : 1
                }
            )

        for entry in results:
            entry["_id"] = str(entry["_id"])
            entry["creation_time"] = str(entry["creation_time"])
            files.append(entry)

        return files


    def get_files_by_data_type(self, user_id, data_type, rest=False):
        """
        Get a list of the file dictionary objects given a `user_id` and
        `data_type`

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
        data_type : str
            The type of information in the file (RNA-seq, ChIP-seq, etc)

        Returns
        -------
        dict
            file_path : str
                Location of the file in the file system
            file_type : str
                File format ("fastq", "fasta", "bam", "bed", "bb", "hdf5",
                "tsv", "gz", "tbi", "wig", "bw", "pdb", "gem", "bt2", "amb",
                "ann", "bwt", "pac", "sa", "tif", 'lif', "prmtop", "trj", "dcd")
            data_type : str
                The type of information in the file (RNA-seq, ChIP-seq, etc)
            taxon_id : int
                Taxon ID that the species that the file has been derived from
            compressed : str
                Type of compression (None, gzip, zip)
            source_id : list
                List of IDs of files that were processed to generate this file
            meta_data : dict
                Dictionary object containing the extra data related to the
                generation of the file or describing the way it was processed
            creation_time : list
                    Time at which the file was loaded into the system

        Example
        -------
        .. code-block:: python
           :linenos:

           from dmp import dmp
           da = dmp()
           da.get_files_by_data_type(<user_id>, <data_type>)
        """
        entries = self.db.entries
        files = []

        if rest is True:
            results = entries.find(
                {"user_id" : user_id, "data_type" : data_type},
                {
                    "file_type" : 1, "data_type" : 1, "taxon_id" : 1,
                    "source_id" : 1, "meta_data" : 1, "creation_time" : 1
                }
            )
        else:
            results = entries.find(
                {"user_id" : user_id, "data_type" : data_type},
                {
                    "file_path" : 1, "file_type" : 1, "data_type" : 1, "taxon_id" : 1,
                    "source_id" : 1, "meta_data" : 1, "creation_time" : 1
                }
            )
        for entry in results:
            entry["_id"] = str(entry["_id"])
            entry["creation_time"] = str(entry["creation_time"])
            files.append(entry)

        return files


    def get_files_by_taxon_id(self, user_id, taxon_id, rest=False):
        """
        Get a list of the file dictionary objects given a `user_id` and
        `taxon_id`

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
        taxon_id : int
            Taxon ID that the species that the file has been derived from

        Returns
        -------
        dict
            file_path : str
                Location of the file in the file system
            file_type : str
                File format ("fastq", "fasta", "bam", "bed", "bb", "hdf5",
                "tsv", "gz", "tbi", "wig", "bw", "pdb", "gem", "bt2", "amb",
                "ann", "bwt", "pac", "sa", "tif", 'lif', "prmtop", "trj", "dcd")
            data_type : str
                The type of information in the file (RNA-seq, ChIP-seq, etc)
            taxon_id : int
                Taxon ID that the species that the file has been derived from
            compressed : str
                Type of compression (None, gzip, zip)
            source_id : list
                List of IDs of files that were processed to generate this file
            meta_data : dict
                Dictionary object containing the extra data related to the
                generation of the file or describing the way it was processed
            creation_time : list
                    Time at which the file was loaded into the system

        Example
        -------
        .. code-block:: python
           :linenos:

           from dmp import dmp
           da = dmp()
           da.get_files_by_taxon_id(<user_id>, <taxon_id>)
        """
        entries = self.db.entries
        files = []

        if rest is True:
            results = entries.find(
                {"user_id" : user_id, "taxon_id" : taxon_id},
                {
                    "file_type" : 1, "data_type" : 1, "taxon_id" : 1,
                    "source_id" : 1, "meta_data" : 1, "creation_time" : 1
                }
            )
        else:
            results = entries.find(
                {"user_id" : user_id, "taxon_id" : taxon_id},
                {
                    "file_path" : 1, "file_type" : 1, "data_type" : 1, "taxon_id" : 1,
                    "source_id" : 1, "meta_data" : 1, "creation_time" : 1
                }
            )

        for entry in results:
            entry["_id"] = str(entry["_id"])
            entry["creation_time"] = str(entry["creation_time"])
            files.append(entry)

        return files


    def _get_file_parents(self, user_id, file_id):
        """
        Private function for getting all parents on a file_id. This function
        reursively goes up the tree of parents to get a full history.

        Parameters
        ----------
        file_id : str
            File ID for leafe file

        Returns
        -------
        file_ids : list
            List of parent file_ids
        """
        entries = self.db.entries
        file_obj = entries.find_one(
            {'user_id' : user_id, '_id': ObjectId(file_id)}, {"source_id" : 1}
        )

        parent_files = []
        if file_obj['source_id']:
            source_count = len(file_obj['source_id'])
            if source_count > 0:
                for source_id in file_obj['source_id']:
                    parent_files.append([file_id, str(source_id)])
                    parent_files += self._get_file_parents(user_id, source_id)

        return parent_files


    def get_file_history(self, user_id, file_id):
        """
        Returns the full path of file_ids from the current file to the original
        file(s)

        Needs work to define the format for how declaring the history is best

        Parameters
        ----------
        file_id : str
            ID of the file. This is the value returned when a file is loaded
            into the DMP or is the `_id` for a given file when the files have
            been retrieved.

        Returns
        -------
        list
            List of lists representing the adjancency of child and parent files.

        Example
        -------
        .. code-block:: python
           :linenos:

           from dmp import dmp
           da = dmp()
           history = da.get_file_history("aLongString")
           print history

        Output:
        ``[['aLongString', 'parentOfaLongString'], ['parentOfaLongString', 'parentOfParent']]``

        These IDs can then be requested to ruturn the meta data and locations
        with the `get_file_by_id` method.
        """

        unique_data = [list(x) for x in set(tuple(x) for x in self._get_file_parents(user_id, file_id))]
        return unique_data


    def remove_file(self, file_id):
        """
        Removes a single file from the directory. Returns the ID of the file
        that was removed

        Parameters
        ----------
        file_id : str
            ID of the file. This is the value returned when a file is loaded
            into the DMP or is the `_id` for a given file when the files have
            been retrieved.

        Returns
        -------
        str
            The file_id of the removed file.

        Example
        -------
        .. code-block:: python
           :linenos:

           from dmp import dmp
           da = dmp()
           da.remove_file(<file_id>)
        """
        self.db.entries.delete_one({'_id': ObjectId(file_id)})
        return file_id


    @staticmethod
    def validate_file(entry):
        """
        Validate that the required meta data for a given entry is present. If
        there is missing data then a ValueError excepetion is raised. This
        function checks that all required paths are defined and that when
        various selections are made then the correct matching data is also
        present

        Parameters
        ----------
        entry : dict
            user_id : str
                Identifier to uniquely locate the users files. Can be set to
                "common" if the files can be shared between users
            file_path : str
                Location of the file in the file system
            file_type : str
                File format ("fastq", "fasta", "bam", "bed", "bb", "hdf5", "tsv",
                "gz", "tbi", "wig", "bw", "pdb", "tif", "lif", "gem", "bt2",
                "amb", "ann", "bwt", "pac", "sa", "prmtop", "trj", "dcd")
            data_type : str
                The type of information in the file (RNA-seq, ChIP-seq, etc)
            taxon_id : int
                Taxon ID that the species that the file has been derived from
            compressed : str
                Type of compression (None, gzip, zip)
            source_id : list
                List of IDs of files that were processed to generate this file
            meta_data : dict
                Dictionary object containing the extra data related to the
                generation of the file or describing the way it was processed
                assembly : string

        Returns
        -------
        bool
            Returns True if there are no errors with the entry

        If there are issues with the entry then a ValueError is raised.
        """

        # Check the user_id is not empty:
        if 'user_id' not in entry or entry['user_id'] is None or entry['user_id'] == '':
            raise ValueError('User ID must be specified for all entries')

        # Check the file_id is not empty:
        if 'file_path' not in entry or entry['file_path'] is None or entry['file_path'] == '':
            raise ValueError('User ID must be specified for all entries')

        # Defined list of acepted file types
        file_types = [
            "fastq", "fa", "fasta", "bam", "bed", "bb", "hdf5", "tsv",
            "gz", "tbi", "wig", "bw", "pdb", "tif", 'lif', "gem", "bt2", "amb",
            "ann", "bwt", "pac", "sa", "prmtop", "trj", "dcd"
        ]

        # Check all files match the defined types
        if  (
                'file_type' not in entry or
                entry['file_type'] == "" or
                entry['file_type'] not in file_types
        ):
            raise ValueError(
                "File type must be one of the valid file types: " + ','.join(file_types)
            )

        # Check all files have a matching Taxon ID
        if 'taxon_id' not in entry or entry['taxon_id'] is None:
            raise ValueError('Taxon ID must be specified for all entries')

        # Require assembly in the meta_data
        if entry['file_type'] in ["fa", "fasta", "bam", "bed", "bb", "hdf5", "tbi", "wig", "bw"]:
            if 'meta_data' not in entry or 'assembly' not in entry['meta_data']:
                raise ValueError(
                    'Matching assembly ID is required within the meta_data field'
                )

        return True


    def set_file( # pylint: disable=too-many-arguments
            self, user_id, file_path, file_type="", data_type="", taxon_id="",
            compressed=None, source_id=None, meta_data=None, **kwargs
        ):
        """
        Adds a file to the data management API.

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
        file_path : str
            Location of the file in the file system
        file_type : str
            File format ("fastq", "fasta", "bam", "bed", "bb", "hdf5", "tsv",
            "gz", "tbi", "wig", "bw", "pdb", "tif", "lif", "gem", "bt2", "amb",
                "ann", "bwt", "pac", "sa", "trj", "dcd")
        data_type : str
            The type of information in the file (RNA-seq, ChIP-seq, etc)
        taxon_id : int
            Taxon ID that the species that the file has been derived from
        compressed : str
            Type of compression (None, gzip, zip)
        source_id : list
            List of IDs of files that were processed to generate this file
        meta_data : dict
            Dictionary object containing the extra data related to the
            generation of the file or describing the way it was processed

            assembly : string
                Dependent paramenter. If the sequence has been aligned at some
                point during the production of this file then the assembly must
                be recorded.

        Returns
        -------
        str
            This is an id for that file within the system and can be used for
            tracing this file and where it is used and where it has come from.

        Example
        -------
        .. code-block:: python
           :linenos:

           from dmp import dmp
           da = dmp()
           unique_file_id = da.set_file(
               'user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq', 9606, None)

        If there is a processed result of 1 or more files then these can be
        specified using the file_id:

        >>> da.set_file(
            'user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq', 9606, None,
            source_id=[1, 2])

        Meta data about the file can also be included to provide extra
        information about the file, origins or how it was generated:

        >>> da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq',
            9606, None, meta_data={'assembly' : 'GCA_0000nnnn',
            'downloaded_from' : 'http://www.', })
        """

        entry = {
            "user_id"       : user_id,
            "file_path"     : file_path,
            "file_type"     : file_type,
            "data_type"     : data_type,
            "taxon_id"      : taxon_id,
            "compressed"    : compressed,
            "source_id"     : source_id,
            "meta_data"     : meta_data,
            "creation_time" : datetime.datetime.utcnow()
        }
        entry.update(kwargs)

        self.validate_file(entry)

        entries = self.db.entries
        entry_id = entries.insert_one(entry).inserted_id
        return str(entry_id)


    def add_file_metadata(self, file_id, key, value):
        """
        Add a key value pair to the meta data for a file

        This way a user is able to add extra information to the meta data to
        better describe the file.

        Parameters
        ----------
        file_id : str
            ID of the file. This is the value returned when a file is loaded
            into the DMP or is the `_id` for a given file when the files have
            been retrieved.
        key : str
            Unique key for the identification of the extra meta data. If the key
            matches a value already in the meta data then it over-writes the
            current value.
        value
            Value to be stored for the given key. This can be a str, int, list
            or dict.

        Returns
        -------
        str
            This is an id for that file within the system and can be used for
            tracing this file and where it is used and where it has come from.
        """

        entries = self.db.entries
        metadata = entries.find_one({'_id': ObjectId(file_id)}, {"meta_data" : 1})
        metadata['meta_data'][str(key)] = value
        entries.update({'_id': ObjectId(file_id)}, {'$set' : {'meta_data' : metadata['meta_data']}})

        return file_id


    def remove_file_metadata(self, file_id, key):
        """
        Remove a key value pair from the meta data for a given file

        Parameters
        ----------
        file_id : str
            ID of the file. This is the value returned when a file is loaded
            into the DMP or is the `_id` for a given file when the files have
            been retrieved.
        key : str
            Unique key for the identification of the extra meta data to be
            removed
        Returns
        -------
        str
            This is an id for that file within the system and can be used for
            tracing this file and where it is used and where it has come from.
        """

        entries = self.db.entries
        metadata = entries.find_one({'_id': ObjectId(file_id)}, {"meta_data" : 1})
        del metadata['meta_data'][key]
        entries.update({'_id': ObjectId(file_id)}, {'$set' : {'meta_data' : metadata['meta_data']}})

        return file_id
