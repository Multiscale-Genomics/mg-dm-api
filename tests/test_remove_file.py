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
    Test to determine that a user has N files and that removing a file reduces
    those stored within the API has reduced by 1.
    """
    user = "adam"

    dm_handle = dmp(test=True)

    results = dm_handle.get_files_by_user(user)
    assert isinstance(results, type([])) is True

    original_number_of_files = len(results)

    dm_handle.remove_file(user, results[0]['_id'])
    results = dm_handle.get_files_by_user(user)

    assert original_number_of_files-1 == len(results)
