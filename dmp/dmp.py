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
import bson
from bson.objectid import ObjectId

from dm_generator.GenerateSampleCoords import GenerateSampleCoords
from dm_generator.GenerateSampleAdjacency import GenerateSampleAdjacency


class dmp(object):  # pylint: disable=invalid-name
    """
    API for management of files within the VRE
    """

    def __init__(self, cnf_loc='', test=False):
        """
        Initialise the module and setup parameters
        """

        config = configparser.RawConfigParser()
        config.read(cnf_loc)

        if test is True:
            import mongomock
            self.client = mongomock.MongoClient()
            self.db_handle = self.client["dmp"]
            self._test_loading_dataset()
        else:
            host = config.get("dmp", "host")
            port = config.getint("dmp", "port")
            user = config.get("dmp", "user")
            password = config.get("dmp", "pass")
            dmp_db = config.get("dmp", "db")

            try:
                self.client = MongoClient(
                    host, port,
                    read_preference=ReadPreference.SECONDARY_PREFERRED
                )
                self.client.admin.authenticate(user, password)
                self.db_handle = self.client[dmp_db]
            except RuntimeError:
                error = sys.exc_info()[0]
                print("Error: %s" % error)
                sys.exit(1)

        self.entries = self.db_handle.entries
        self.db_handle.entries.create_index(
            [('user_id', pymongo.ASCENDING)],
            unique=False, background=True)
        self.db_handle.entries.create_index(
            [('user_id', pymongo.ASCENDING), ('file_type', pymongo.ASCENDING)],
            unique=False, background=True)
        self.db_handle.entries.create_index(
            [('user_id', pymongo.ASCENDING), ('data_type', pymongo.ASCENDING)],
            unique=False, background=True)
        self.db_handle.entries.create_index(
            [('user_id', pymongo.ASCENDING), ('taxon_id', pymongo.ASCENDING)],
            unique=False, background=True)

    def _copy_to_tmp(self, file_path, tmp_path):

        if os.path.isfile(tmp_path) is False:
            with open(tmp_path, 'wb') as f_out:
                with open(file_path, 'rb') as f_in:
                    f_out.write(f_in.read())

        return True

    def _test_loading_dataset(self):
        users = ["adam", "ben", "chris", "denis", "eric"]
        file_types = [
            "fastq", "fa", "fasta", "bam", "bed", "bb", "hdf5", "tsv", "gz",
            "tbi", "wig", "bw"
        ]
        data_types = ['RNA-seq', 'MNase-Seq', 'ChIP-seq', 'WGBS', 'HiC']
        compressed = [None, 'gzip', 'zip']
        resource_path = os.path.dirname(__file__)

        file_id = self.set_file(
            "rao", os.path.join(resource_path, 'rao2014.hdf5'),
            "file", "hdf5", 64000, None, "HiC", 9606,
            meta_data={'assembly': 'GCA_0123456789'}
        )

        data_path = os.path.join(os.path.dirname(__file__), "../tests/data/")

        file_id = self.set_file(
            "test", os.path.join('/tmp/sample.bb'),
            "file", "bb", 64000, None, "RNA-seq", 9606,
            meta_data={'assembly': 'GCA_0123456789'},
            _id=ObjectId(str("testtest0000"))
        )
        self._copy_to_tmp(data_path + 'sample.bb', '/tmp/sample.bb')

        file_id = self.set_file(
            "test", os.path.join('/tmp/sample.bw'),
            "file", "bw", 64000, None, "RNA-seq", 9606,
            meta_data={'assembly': 'GCA_0123456789'},
            _id=ObjectId(str("testtest0001"))
        )
        self._copy_to_tmp(data_path + 'sample.bw', '/tmp/sample.bw')

        file_id = self.set_file(
            "test", '/tmp/sample_coords.hdf5',
            "file", "hdf5", 64000, None, "HiC", 9606,
            meta_data={'assembly': 'GCA_0123456789'},
            _id=ObjectId(str("testtest0002"))
        )
        if os.path.isfile('/tmp/sample_coords.hdf5') is False:
            gsc = GenerateSampleCoords()
            gsc.main()

        file_id = self.set_file(
            "test", '/tmp/sample_adjacency.hdf5',
            "file", "hdf5", 64000, None, "HiC", 9606,
            meta_data={'assembly': 'GCA_0123456789'},
            _id=ObjectId(str("testtest0003"))
        )
        if os.path.isfile('/tmp/sample_adjacency.hdf5') is False:
            gsa = GenerateSampleAdjacency()
            gsa.main()

        for user in users:
            data_type = 'RNA-seq'
            file_handle = '/tmp/test/' + data_type + '/test_rna-seq.fastq'
            file_type = "fastq"
            zipped = None
            file_id = self.set_file(
                user, file_handle, "file", file_type, 64000, None, data_type, 9606, None, None,
                meta_data={'assembly': 'GCA_0123456789'})
            file_handle = '/tmp/test/' + data_type + '/test_rna-seq.bam'
            self.set_file(
                user, file_handle, "file", 'bam', 64000, None, data_type, 9606, None, [file_id],
                meta_data={'assembly': 'GCA_0123456789', 'tool': 'bwa_aligner'})

        for i in range(10):
            user = random.choice(users)
            file_type = random.choice(file_types)
            data_type = random.choice(data_types)
            zipped = random.choice(compressed)
            file_handle = '/tmp/test/' + data_type + '/test_' + str(i) + '.' + file_type
            file_id = self.set_file(
                user, file_handle, "file", file_type, 64000, None, data_type, 9606, zipped,
                meta_data={'assembly': 'GCA_0123456789'})

            if data_type == 'RNA-seq' and file_type == 'fastq' and random.choice([0, 1]) == 1:
                file_handle = '/tmp/test/' + data_type + '/test_' + str(i) + '.bam'
                self.set_file(
                    user, file_handle, "file", 'bam', 64000, None,
                    data_type, 9606, None, [file_id],
                    meta_data={'assembly': 'GCA_0123456789', 'tool': 'bwa_aligner'})

    def _get_rows(self, user_id, key=None, value=None, rest=False):
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
                File format (see validate_file)
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
        entries = self.db_handle.entries
        files = []

        row_filter = {"user_id": user_id}
        if (
                key is not None
                and isinstance(str(key), str)
                and isinstance(value, (str, int, float, bson.objectid.ObjectId))
        ):
            row_filter[key] = value

        if rest is True:
            results = entries.find(
                row_filter,
                {
                    "file_type": 1, "size": 1, "data_type": 1, "taxon_id": 1,
                    "source_id": 1, "meta_data": 1, "creation_time": 1
                }
            )
        else:
            results = entries.find(
                row_filter,
                {
                    "file_path": 1, "path_type": 1, "file_type": 1, "size": 1,
                    "parent_dir": 1, "data_type": 1, "taxon_id": 1,
                    "source_id": 1, "meta_data": 1, "creation_time": 1
                }
            )

        for entry in results:
            entry["_id"] = str(entry["_id"])
            entry["creation_time"] = str(entry["creation_time"])
            if "expiration_date" in entry["meta_data"]:
                entry["meta_data"]["expiration_date"] = str(entry["meta_data"]["expiration_date"])
            files.append(entry)

        return files

    def get_file_by_id(self, user_id, file_id, rest=False):
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
            path_type : str

            file_type : str
                File format (see validate_file)
            size : int
            parent_dir : str
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
        file_obj = self._get_rows(str(user_id), '_id', ObjectId(str(file_id)), rest)

        if len(file_obj) == 0:
            return {}

        return file_obj[0]

    def get_file_by_file_path(self, user_id, file_path, rest=False):
        """
        Get a list of the file dictionary objects given a `user_id` and
        `file_path`

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
        file_path : str
            File path (see validate_file)

        Returns
        -------
        dict
            file_path : str
                Location of the file in the file system
            file_type : str
                File format (see validate_file)
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
           da.get_files_by_file_path(<user_id>, <file_type>)
        """
        return self._get_rows(str(user_id), 'file_path', str(file_path), rest)


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
        return self._get_rows(str(user_id), None, None, rest)

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
            File format (see validate_file)

        Returns
        -------
        dict
            file_path : str
                Location of the file in the file system
            file_type : str
                File format (see validate_file)
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
        return self._get_rows(str(user_id), "file_type", str(file_type), rest)

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
                File format (see validate_file)
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
        return self._get_rows(str(user_id), "data_type", str(data_type), rest)

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
                File format (see validate_file)
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
        return self._get_rows(str(user_id), "taxon_id", int(taxon_id), rest)

    def get_files_by_assembly(self, user_id, assembly, rest=False):
        """
        Get a list of the file dictionary objects given a `user_id` and
        `assembly`

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
        assembly : str
            Assembly that the species that the file has been derived from

        Returns
        -------
        dict
            file_path : str
                Location of the file in the file system
            file_type : str
                File format (see validate_file)
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
        return self._get_rows(str(user_id), "meta_data.assembly", str(assembly), rest)

    def _get_file_parents(self, user_id, file_id):
        """
        Private function for getting all parents on a file_id. This function
        reursively goes up the tree of parents to get a full history.

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
        file_id : str
            File ID for leafe file

        Returns
        -------
        file_ids : list
            List of parent file_ids
        """
        entries = self.db_handle.entries
        file_obj = entries.find_one(
            {'user_id': user_id, '_id': ObjectId(file_id)}, {"source_id": 1}
        )

        parent_files = []
        if file_obj is not None and file_obj['source_id']:
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
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
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
        unique_data = [
            list(x) for x in set(tuple(x) for x in self._get_file_parents(user_id, file_id))
        ]

        return unique_data

    def remove_file(self, user_id, file_id):
        """
        Removes a single file from the directory. Returns the ID of the file
        that was removed

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
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
        self.db_handle.entries.delete_one({'user_id': user_id, '_id': ObjectId(file_id)})
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
            path_type : str

            file_type : str
                File format ("amb", "ann", "bam", "bb", "bed", "bt2", "bw",
                "bwt", "cpt", "csv", "dcd", "fa", "fasta", "fastq", "gem",
                "gff3", "gz", "hdf5", "json", 'lif', "pac", "pdb", "pdf", "png",
                "prmtop", "sa", "tbi", "tif", "tpr", "trj", "tsv", "txt", "wig")
            size : int
                Size of the file in bytes
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

        if entry['path_type'] not in ['file', 'dir', 'link']:
            raise ValueError('Path type must be of value file|dir|link')

        # Defined list of acepted file types
        file_types = {
            "amb": ["assembly"],
            "ann": ["assembly"],
            "bam": ["assembly"],
            "bb": ["assembly"],
            "bed": ["assembly"],
            "bt2": ["assembly"],
            "bw": [],
            "bwt": ["assembly"],
            "cpt": [],
            "csv": [],
            "dcd": [],
            "fa": [],
            "fasta": ["assembly"],
            "fastq": [],
            "gem": ["assembly"],
            "gff3": ["assembly"],
            "gz": [],
            "hdf5": ["assembly"],
            "json": [],
            'lif': [],
            "pac": ["assembly"],
            "pdb": [],
            "pdf": [],
            "png": [],
            "prmtop": [],
            "sa": ["assembly"],
            "tbi": ["assembly"],
            "tif": [],
            "tpr": [],
            "trj": [],
            "tsv": [],
            "txt": [],
            "wig": ["assembly"]
        }

        # Check all files match the defined types
        if (
                'file_type' not in entry or
                entry['file_type'] == "" or
                entry['file_type'] not in file_types
        ):
            raise ValueError(
                "File type must be one of the valid file types: " + ','.join(file_types)
            )

        if isinstance(entry['size'], int) is False:
            raise TypeError('Size must be an integer')

        # Check all files have a matching Taxon ID
        if 'taxon_id' not in entry or entry['taxon_id'] is None:
            raise ValueError('Taxon ID must be specified for all entries')

        # Require assembly in the meta_data
        ft_assembly_required = [k for k in file_types if "assembly" in file_types[k]]
        if entry['file_type'] in ft_assembly_required:
            if 'meta_data' not in entry or 'assembly' not in entry['meta_data']:
                raise ValueError(
                    'Matching assembly ID is required within the meta_data field'
                )

        if entry['source_id'] is not None:
            if 'meta_data' not in entry or 'tool' not in entry['meta_data']:
                raise ValueError(
                    'Matching Tool name is required within the meta_data field'
                )

        return True

    def set_file(  # pylint: disable=too-many-arguments
            self, user_id, file_path, path_type, file_type="", size=0, parent_dir="", data_type="",
            taxon_id="", compressed=None, source_id=None, meta_data=None, **kwargs):
        """
        Adds a file to the data management API.

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
        file_path : str
            Location of the file in the file system
        path_type : str

        parent_dir : str
            _id of the parent directory
        file_type : str
            File format (see validate_file)
        size : int
            File size in bytes
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
            "user_id": user_id,
            "file_path": file_path,
            "path_type": path_type,
            "parent_dir": parent_dir,
            "file_type": file_type,
            "size": size,
            "data_type": data_type,
            "taxon_id": taxon_id,
            "compressed": compressed,
            "source_id": source_id,
            "meta_data": meta_data,
            "creation_time": datetime.datetime.utcnow()
        }
        date_delta = datetime.timedelta(days=84)
        entry["meta_data"]["expiration_date"] = entry["creation_time"] + date_delta
        entry.update(kwargs)

        self.validate_file(entry)

        entries = self.db_handle.entries
        entry_id = entries.insert_one(entry).inserted_id
        return str(entry_id)

    def add_file_metadata(self, user_id, file_id, key, value):
        """
        Add a key value pair to the meta data for a file

        This way a user is able to add extra information to the meta data to
        better describe the file.

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
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
        entries = self.db_handle.entries
        entry = entries.find_one(
            {'user_id': user_id, '_id': ObjectId(file_id)}
        )
        entry['meta_data'][str(key)] = value

        # Check that the changes are still valid
        self.validate_file(entry)

        entries.update(
            {'_id': ObjectId(file_id)},
            {'$set': {'meta_data': entry['meta_data']}}
        )

        return file_id

    def remove_file_metadata(self, user_id, file_id, key):
        """
        Remove a key value pair from the meta data for a given file

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
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
        entries = self.db_handle.entries
        entry = entries.find_one(
            {'user_id': user_id, '_id': ObjectId(file_id)}
        )
        del entry['meta_data'][key]

        # Check that the changes are still valid
        self.validate_file(entry)

        entries.update(
            {'user_id': user_id, '_id': ObjectId(file_id)},
            {'$set': {'meta_data': entry['meta_data']}}
        )

        return file_id

    def modify_column(self, user_id, file_id, key, value):
        """
        Update a key value pair for the record

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
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
        entries = self.db_handle.entries
        entry = entries.find_one(
            {'user_id': user_id, '_id': ObjectId(file_id)}
        )
        if str(key) in ['size', 'taxon_id']:
            entry[str(key)] = int(value)
        else:
            entry[str(key)] = value

        # Check that the changes are still valid
        self.validate_file(entry)

        # Update the entry witin the mongodb
        entries.update(
            {'user_id': user_id, '_id': ObjectId(file_id)},
            {'$set': {str(key): entry[str(key)]}}
        )

        return file_id
