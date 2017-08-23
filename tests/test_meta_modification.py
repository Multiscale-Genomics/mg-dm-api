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

import pytest # pylint: disable=unused-import

from dmp import dmp

def test_files_by_user():
    """
    Test that it is possible to add and then remove a piece of meta data from
    a pre-existing file within the DM API.
    """
    user = "adam"

    dm_handle = dmp(test=True)

    results = dm_handle.get_files_by_user(user)
    assert isinstance(results, type([])) is True

    file_id = dm_handle.add_file_metadata(results[0]['_id'], 'test', 'An example string')
    result = dm_handle.get_file_by_id(user, file_id)
    assert 'test' in result['meta_data'].keys()

    dm_handle.remove_file_metadata(file_id, 'test')
    result = dm_handle.get_file_by_id(user, file_id)
    assert 'test' not in result['meta_data'].keys()
