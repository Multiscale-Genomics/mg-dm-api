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

class rest:
    """
    API for management of files within the VRE
    """
    
    def __init__(self):
        """
        Initialise the module and 
        """
        config = ConfigParser.RawConfigParser()
        config.read('mongodb.cnf')
        
        host = config.get("rest", "host")
        port = config.getint("rest", "port")
        user = config.get("rest", "user")
        password = config.get("rest", "pass")
        db = config.get("rest", "db")
        
        self.client = MongoClient()
        self.client = MongoClient(host, port)
        self.db = self.client[rest_db]
        self.db.authenticate(user, password)
        
        self.entries = self.db.entries
        self.db.entries.create_index([('user_id', pymongo.ASCENDING)], unique=False)
        self.db.entries.create_index([('user_id', pymongo.ASCENDING), ('file_type', pymongo.ASCENDING)], unique=False)
        self.db.entries.create_index([('user_id', pymongo.ASCENDING), ('data_type', pymongo.ASCENDING)], unique=False)
        self.db.entries.create_index([('user_id', pymongo.ASCENDING), ('taxon_id', pymongo.ASCENDING)], unique=False)
    
    
    def get_service(self, name):
        """
        
        """
        return 1
        
    
    def add_service(self, name, url, description, status=None):
        """
        
        """
        entry = {
            "name"        : name,
            "description" : description,
            "url"         : url,
            "status"      : status,
        }
        
        entries = self.db.entries
        entry_id = entries.insert_one(entry).inserted_id
        return str(entry_id)
    
    
    def set_service_status(self, name, status):
        """
        
        """
        return 1
