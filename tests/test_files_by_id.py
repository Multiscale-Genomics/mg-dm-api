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

from dmp import dmp

from bson.objectid import ObjectId

def test_files_by_id():
    """
    Test the retrieval of files for users by file type
    """
    user = "test"
    file_paths = [
        ObjectId(str("testtest0000")),
        ObjectId(str("testtest0001")),
        ObjectId(str("testtest0002")),
        ObjectId(str("testtest0003"))
    ]

    dm_handle = dmp(test=True)

    for file_path in file_paths: # pylint: disable=unused-variable
        result = dm_handle.get_file_by_id(user, file_path)
        print("DMP RESULTS:", result)
        assert isinstance(result, type({})) is True
        assert 'file_path' in result


# def test_files_by_type_id():
#     """
#     Test the retrieval of files for users by file type
#     """
#     user = "test"
#     file_paths = ["/tmp/sample.bb", "/tmp/sample.bw"]

#     dm_handle = dmp(test=True)

#     for file_path in file_paths: # pylint: disable=unused-variable
#         results = dm_handle.get_file_by_file_path(user, file_path)
#         assert isinstance(results, type([])) is True
#         assert len(results) == 0
#         for result in results:
#             assert 'file_path' not in result
