"""
.. Copyright 2016 EMBL-European Bioinformatics Institute

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

import datetime, ConfigParser, random, os
import mongomock
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId

class dmp:
    """
    API for management of files within the VRE
    """
    
    def __init__(self, test = False):
        """
        Initialise the module and 
        """
        
        config = ConfigParser.RawConfigParser()
        config.read('mongodb.cnf')
        
        host = config.get("dmp", "host")
        port = config.getint("dmp", "port")
        user = config.get("dmp", "user")
        password = config.get("dmp", "pass")
        dmp_db = config.get("dmp", "db")
        self.ftp_root = config.get("dmp", "ftp_root")
        
        if test == True:
            self.client = mongomock.MongoClient()
            self.db = self.client[dmp_db]
            self._test_loading_dataset()
        else:    
            self.client = MongoClient()
            self.client = MongoClient(host, port)
            self.db = self.client[dmp_db]
            self.db.authenticate(user, password)
        
        self.entries = self.db.entries
        self.db.entries.create_index([('user_id', pymongo.ASCENDING)], unique=False)
        self.db.entries.create_index([('user_id', pymongo.ASCENDING), ('file_type', pymongo.ASCENDING)], unique=False)
        self.db.entries.create_index([('user_id', pymongo.ASCENDING), ('data_type', pymongo.ASCENDING)], unique=False)
        self.db.entries.create_index([('user_id', pymongo.ASCENDING), ('taxon_id', pymongo.ASCENDING)], unique=False)
        
    
    def _test_loading_dataset(self):
        users = ["adam", "ben", "chris", "denis", "eric"]
        file_types = ["fastq", "fasta", "bam", "bed", "hdf5", "tsv", "wig", "pdb"]
        data_types = ['RNA-seq', 'MNase-Seq', 'ChIP-seq', 'WGBS', 'HiC']
        compressed = [None, 'gzip', 'zip']
        
        resource_package = __name__
        resource_path = os.path.join(os.path.dirname(__file__), 'rao2014.hdf5')
        file_id = self.set_file("rao", resource_path, "hdf5", "HiC", 9606, None)
        
        for i in xrange(10):
            u = random.choice(users)
            ft = random.choice(file_types)
            dt = random.choice(data_types)
            z  = random.choice(compressed)
            f = '/tmp/test/' + dt + '/test_' + str(i) + '.' + ft
            file_id = self.set_file(u, f, ft, dt, 9606, z)
            
            if dt == 'RNA-seq' and ft == 'fastq' and random.choice([0,1]) == 1:
                 f = '/tmp/test/' + dt + '/test_' + str(i) + '.bam'
                 self.set_file(u, f, 'bam', dt, 9606, None, [file_id])
    
    
    def get_file_by_id(self, user_id, file_id, rest = False):
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
                File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
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
        >>> from dmp import dmp
        >>> da = dmp.dmp()
        >>> da.get_file_by_id(<unique_file_id>)
        """
        entries = self.db.entries
        file_obj = entries.find_one({'_id': ObjectId(file_id), 'user_id': user_id})
        return file_obj
    
    
    def get_files_by_user(self, user_id, rest = False):
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
        >>> from dmp import dmp
        >>> da = dmp.dmp()
        >>> da.get_files_by_user(<user_id>)
        """
        entries = self.db.entries
        files = []
        for entry in entries.find({"user_id" : user_id}, {"file_path" : 1, "file_type" : 1, "data_type" : 1, "taxon_id" : 1, "source_id" : 1, "meta_data" : 1, "creation_time" : 1}):
            if rest == True:
                file_path = str(entry["file_path"]).split("/")
                url = "/".join([self.ftp_root, str(entry["_id"])] + file_path[-2:])
                entry = url
            files.append(entry)
        return files
    
    
    def get_files_by_file_type(self, user_id, file_type):
        """
        Get a list of the file dictionary objects given a `user_id` and
        `file_type`
        
        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to 
            "common" if the files can be shared between users
        file_type : str
            File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
        
        Returns
        -------
        dict
            file_path : str
                Location of the file in the file system
            file_type : str
                File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
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
        >>> from dmp import dmp
        >>> da = dmp.dmp()
        >>> da.get_files_by_file_type(<user_id>, <file_type>)
        """
        entries = self.db.entries
        files = []
        for entry in entries.find({"user_id" : user_id, "file_type" : file_type}):
            files.append(entry)
        return files
    
    
    def get_files_by_data_type(self, user_id, data_type):
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
                File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
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
        >>> from dmp import dmp
        >>> da = dmp.dmp()
        >>> da.get_files_by_data_type(<user_id>, <data_type>)
        """
        entries = self.db.entries
        files = []
        for entry in entries.find({"user_id" : user_id, "data_type" : data_type}, {"file_path" : 1, "file_type" : 1, "data_type" : 1, "taxon_id" : 1, "source_id" : 1, "meta_data" : 1, "creation_time" : 1}):
            files.append(entry)
        return files
    
    
    def get_files_by_taxon_id(self, user_id, taxon_id):
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
                File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
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
        >>> from dmp import dmp
        >>> da = dmp.dmp()
        >>> da.get_files_by_taxon_id(<user_id>, <taxon_id>)
        """
        entries = self.db.entries
        files = []
        for entry in entries.find({"user_id" : user_id, "taxon_id" : taxon_id}, {"file_path" : 1, "file_type" : 1, "data_type" : 1, "taxon_id" : 1, "source_id" : 1, "meta_data" : 1, "creation_time" : 1}):
            files.append(entry)
        return files
    
    
    def _get_file_parents(self, file_id):
        """
        Private function for getting all parents on a file_id. This function
        reursively goes up the tree of parents to get a full history.
        """
        entries = self.db.entries
        file_obj = entries.find_one({'_id': ObjectId(file_id)}, {"source_id" : 1})
        
        parent_files = []
        if len(file_obj['source_id']) > 0:
            for source_id in file_obj['source_id']:
                parent_files.append([file_id, str(source_id)])
                parent_files += self._get_file_parents(source_id)
        
        return parent_files
    
    
    def get_file_history(self, file_id):
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
        >>> from dmp import dmp
        >>> da = dmp.dmp()
        >>> history = da.get_file_history("aLongString")
        >>> print history
        
        Output:
        ``[['aLongString', 'parentOfaLongString'], ['parentOfaLongString', 'parentOfParent']]``
        
        These IDs can then be requested to ruturn the meta data and locations
        with the `get_file_by_id` method.
        """
        unique_data = [list(x) for x in set(tuple(x) for x in self._get_file_parents(file_id))]
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
        >>> from dmp import dmp
        >>> da = dmp.dmp()
        >>> da.remove_file(<file_id>)
        """
        self.db.entries.delete_one({'_id': ObjectId(file_id)})
        return file_id
    
    
    def set_file(self, user_id, file_path, file_type = "", data_type = "", taxon_id = "", compressed=None, source = [], meta_data = {}, **kwargs):
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
            File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
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
            Dependent paramenter. If the sequence has been aligned at some point
            during the production of this file then the assembly must be
            recorded.
        
        Returns
        -------
        str
            This is an id for that file within the system and can be used for
            tracing this file and where it is used and where it has come from.
        
        Example
        -------
        >>> from dmp import dmp
        >>> da = dmp.dmp()
        >>> unique_file_id = da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq', 9606, None)
        
        If the is the processed result of 1 or more files then these can be specified using the file_id:

        >>> da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq', 9606, None, source_id=[1, 2])

        Meta data about the file can also be included to provide extra information about the file, origins or how it was generated:
        
        >>> da.set_file('user1', '/tmp/example_file.fastq', 'fastq', 'RNA-seq', 9606, None, meta_data={'downloaded_from' : 'http://www.', })
        """
        
        entry = {
            "user_id"       : user_id,
            "file_path"     : file_path,
            "file_type"     : file_type,
            "data_type"     : data_type,
            "taxon_id"      : taxon_id,
            "compressed"    : compressed,
            "source_id"     : source,
            "meta_data"     : meta_data,
            "creation_time" : datetime.datetime.utcnow()
        }
        entry.update(kwargs)
        
        entries = self.db.entries
        entry_id = entries.insert_one(entry).inserted_id
        return str(entry_id)
