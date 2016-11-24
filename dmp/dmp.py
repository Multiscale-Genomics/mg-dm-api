"""
Copyright 2016 EMBL-European Bioinformatics Institute

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

import datetime, ConfigParser
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId

class dmp:
    """
    API for management of files within the VRE
    """
    
    def __init__(self):
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
        
        self.client = MongoClient()
        self.client = MongoClient(host, port)
        self.db = self.client[dmp_db]
        self.db.authenticate(user, password)
        
        self.entries = self.db.entries
        self.db.entries.create_index([('user_id', pymongo.ASCENDING)], unique=False)
        self.db.entries.create_index([('user_id', pymongo.ASCENDING), ('file_type', pymongo.ASCENDING)], unique=False)
        self.db.entries.create_index([('user_id', pymongo.ASCENDING), ('data_type', pymongo.ASCENDING)], unique=False)
        
    
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
        file_obj : dict
            user_id : str
                Identifier to uniquely locate the users files. Can be set to 
                "common" if the files can be shared between users
            file_path : str
                Location of the file in the file system
            file_type : str
                File format (fasta, fastq, bam, bed, wig, hdf5, pdf, txt, tsv)
            data_type : str
                The type of information in the file (RNA-seq, ChIP-seq, etc)
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
            from dmp import dmp
            da = dmp()
            da.get_file_by_id(<unique_file_id>)
        """
        entries = self.db.entries
        file_obj = entries.find_one({'_id': ObjectId(file_id), 'user_id': user_id})
        return file_obj
    
    
    def get_files_by_user(self, user_id):
        """
        Return the file data for a given user
        """
        entries = self.db.entries
        files = []
        for entry in entries.find({"user_id" : user_id}):
            files.append(entry)
        return files
    
    
    def get_files_by_file_type(self, user_id, file_type):
        """
        Return the files for a given user based on the user_id and the file type
        """
        entries = self.db.entries
        files = []
        for entry in entries.find({"user_id" : user_id, "file_type" : file_type}):
            files.append(entry)
        return files
    
    
    def get_files_by_data_type(self, user_id, data_type):
        """
        Return the files for a given user based on the user_id and the data type
        """
        entries = self.db.entries
        files = []
        for entry in entries.find({"user_id" : user_id, "data_type" : data_type}):
            files.append(entry)
        return files
    
    
    def _get_file_parents(self, file_id):
        """
        Private function for getting all parents on a file_id. This function
        reursively goes up the tree of parents to get a full history.
        """
        entries = self.db.entries
        file_obj = entries.find_one({'_id': ObjectId(file_id)})
        
        parent_files = []
        if len(file_obj['source_id']) > 0:
            for source_id in file_obj['source_id']:
                parent_files.append(self._get_file_parents(source_id))
        
        return {file_id : parent_files}
    
    
    def get_file_history(self, file_id):
        """
        Returns the full path of file_ids from the current file to the original
        file(s)
        
        Needs work to define the format for how declaring the history is best
        """
        return self._get_file_parents(file_id)
    
    
    def remove_file(self, file_id):
        """
        Remove single files from the directory. Returns the ID of the file that
        was removed
        """
        self.db.entries.delete_one({'_id': ObjectId(file_id)})
        return file_id
    
    
    def set_file(self, user_id, file_path, file_type = "", data_type = "", source = [], meta_data = {}):
        """
        Add file to the list for managing.
        """
        
        entry = {
            "user_id"       : user_id,
            "file_path"     : file_path,
            "file_type"     : file_type,
            "data_type"     : data_type,
            "source_id"     : source,
            "meta"          : meta_data,
            "creation_time" : datetime.datetime.utcnow()
        }
        
        entries = self.db.entries
        entry_id = entries.insert_one(entry).inserted_id
        return str(entry_id)
