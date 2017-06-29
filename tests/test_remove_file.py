"""
.. Copyright 2017 EMBL-European Bioinformatics Institute
   
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

import random
import pytest

from dmp import dmp

def test_files_by_user(capsys):

    user = "adam"

    da = dmp(test=True)

    results = da.get_files_by_user(user)
    assert type(results) == type([])
    
    original_number_of_files = len(results)

    file_id = da.remove_file(results[0]['_id'])
    results = da.get_files_by_user(user)

    assert original_number_of_files-1 == len(results)