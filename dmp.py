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
        
    
    def get_file_by_id(self, id):
        """
        Returns files data based on the unique_id for a given file
        """
        return {"file": file_path, "meta": meta}
    
    
    def get_file_by_type(self, user_id, ):
        """
        Return the file data based on the user and the accession for that file
        """
        entries = db.entries
        entries.find_one()
        return {"file": file_path, "meta": meta}
    
    
    def set_file(self, user_id, file_path, file_type = "", source = [], meta_data = {})
        """
        Add file to the list for managing.
        """
        
        entry = {
            "user_id"       : user_id,
            "file_path"     : file_path,
            "file_type"     : file_type,
            "source"        : source,
            "meta"          : meta_data,
            "creation_time" : datetime.datetime.utcnow()
        }
        
        entries = db.entries
        entry_id = entries.insert_one(entry).inserted_id
        return entry_id
