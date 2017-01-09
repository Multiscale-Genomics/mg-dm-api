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

import datetime, ConfigParser, pymongo
from pymongo import MongoClient, ReadPreference
from bson.objectid import ObjectId

class rest:
    """
    API for management of files within the VRE
    """
    
    def __init__(self, cnf_loc = '', test = False):
        """
        Initialise the module and 
        """
        
        config = ConfigParser.RawConfigParser()
        config.read(cnf_loc)
        
        if test == True:
            import mongomock
            self.client = mongomock.MongoClient()
            self.db = self.client["rest"]
            self._test_loading_dataset()
        else: 
            host = config.get("rest", "host")
            port = config.getint("rest", "port")
            user = config.get("rest", "user")
            password = config.get("rest", "pass")
            db = config.get("rest", "db")
            
            try:
                self.client = MongoClient(host, port, read_preference = ReadPreference.SECONDARY_PREFERRED)
                self.client.admin.authenticate(user, password)
                self.db = self.client[db]
            except:
                e = sys.exc_info()[0]
                print "Error: %s" % e
                sys.exit(1)
        
        self.entries = self.db.entries
        self.db.entries.create_index([('name', pymongo.ASCENDING)], unique=True)
        self.db.entries.create_index([('status', pymongo.ASCENDING)], unique=False)
        
    
    def _test_loading_dataset(self):
        self.add_service("service", "Root API service", "/api", "up")
        self.add_service("dmp", "DMP API - Lists static tracks", "/api/dmp", "up")
        self.add_service("adjacency", "Adjacency - API for accessing Hi-C adjacency files", "/api/adjacency", "up")
    
    
    def get_service(self, name):
        """
        Retreive the full details about a service
        
        Parameters
        ----------
        name : str
            Unique name for the service
        
        Returns
        -------
        dict
            name: str
                Unique name for the service
            description: str
                Description defined by the service
            url: str
                Base URL for the RESET service.
            status: str
                Service HTTP status code - `up` or `down`
        """
        entries = self.db.entries
        result = entries.find_one({'name': name})
        service = {'name': r["name"], 'url': r["url"].replace('/ping', ''), 'description': r["description"]}
        return service
    
    
    def get_available_services(self):
        """
        List all services
        
        Returns
        -------
        list
            List of dict objects for each service
        """
        services = []
        entries = self.db.entries
        results = entries.find()
        for r in results:
            services.append({'name': r["name"], 'url': r["url"].replace('/ping', ''), 'description': r["description"]})
        return services
    
    
    def get_up_services(self):
        """
        List services that are returning HTTP code 200
        
        Returns
        -------
        list
            List of dict objects for each service
        """
        services = []
        entries = self.db.entries
        results = entries.find({'status': 'up'}, {'name': 1, 'url': 1, 'description': 1})
        for r in results:
            services.append({'name': r["name"], 'url': r["url"].replace('/ping', ''), 'description': r["description"]})
        return services
    
    
    def get_down_services(self):
        """
        List services that are NOT returning HTTP code 200
        
        Returns
        -------
        list
            List of dict objects for each service
        """
        services = []
        entries = self.db.entries
        results = entries.find({'status': 'down'}, {'name': 1, 'url': 1, 'description': 1})
        for r in results:
            services.append({'name': r["name"], 'url': r["url"].replace('/ping', ''), 'description': r["description"]})
        return services
    
    
    def is_service(self, name):
        """
        Identify if a service is already present in the registry
        
        Parameters
        ----------
        name : str
            Unique name for the service
        
        Returns
        -------
        
        """
        entries = self.db.entries
        file_obj = entries.find_one({'name': name}, {'name': 1})
        if file_obj == None:
            return False
        return True
        
    
    def add_service(self, name, url, description, status=None):
        """
        Add a service to the registry
        
        Parameters
        ----------
        name : str
            Unique name for the service
        description: str
            Description defined by the service
        url: str
            Base URL for the RESET service.
        status : str
            Service HTTP status code - `up` or `down`
        
        Returns
        -------
        str
            Entry ID
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
        Update the status of the service if it is already present in the db.
        
        Parameters
        ----------
        name : str
            Unique name for the service
        status : str
            Service HTTP status code - `up` or `down`
        
        Returns
        -------
        bool
            `True` when done
        """
        entries = self.db.entries
        entries.update({'name': name}, {'$set': {'status': status}})
        return 1
