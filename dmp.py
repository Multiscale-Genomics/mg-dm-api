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
        port = config.get("dmp", "port")
        user = config.get("dmp", "user")
        password = config.get("dmp", "pass")
        db = config.get("dmp", "db")
        
        self.client = MongoClient()
        self.client = MongoClient(host, port)
        self.db = client[db]
        
    
    def get_file_by_id(self, file_id):
        """
        Returns files data based on the unique_id for a given file
        """
        entries = db.entries
        file_obj = entries.find_one({'_id': ObjectId(file_id)})
        return file_obj
    
    
    def get_files_by_user(self, user_id):
        """
        Return the file data for a given user
        """
        entries = db.entries
        files = []
        for entry in entries.find({"user_id" : user_id}):
            files.append(entry)
        return files
    
    
    def get_files_by_type(self, user_id, file_type):
        """
        Return the files for a given user based on the user_id and the file type
        """
        entries = db.entries
        files = []
        for entry in entries.find({"user_id" : user_id, "file_type" : file_type}):
            files.append(entry)
        return files
    
    
    def get_file_history(self, user_id, file_id):
        """
        Returns the full path of file_ids from teh current file to the original
        file(s)
        
        Needs work to define the format for how declaring the history is best
        """
        return = []
    
    
    def set_file(self, user_id, file_path, file_type = "", data_type = "", source = [], meta_data = {})
        """
        Add file to the list for managing.
        """
        
        entry = {
            "user_id"       : user_id,
            "file_path"     : file_path,
            "file_type"     : file_type,
            "data_type"     : data_type
            "source"        : source,
            "meta"          : meta_data,
            "creation_time" : datetime.datetime.utcnow()
        }
        
        entries = db.entries
        entry_id = entries.insert_one(entry).inserted_id
        return entry_id
