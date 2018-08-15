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

import os

from dmp import dmp


def test_files_by_file_path():
    """
    Test the retrieval of files for users by file type
    """
    user = "test"
    file_name_bb = os.path.realpath(os.path.join(
        os.path.dirname(__file__),
        "../tests/data/sample.bb"
    ))
    file_name_bw = os.path.realpath(os.path.join(
        os.path.dirname(__file__),
        "../tests/data/sample.bw"
    ))
    file_paths = [file_name_bb, file_name_bw]

    dm_handle = dmp(test=True)

    for file_path in file_paths:
        results = dm_handle.get_file_by_file_path(user, file_path)

        assert isinstance(results, type([])) is True
        assert len(results) == 1

        for result in results:
            assert 'file_path' in result


def test_files_by_file_path_rest():
    """
    Test the retrieval of files for users by file type
    """
    user = "test"
    file_name_bb = os.path.realpath(os.path.join(
        os.path.dirname(__file__),
        "../tests/data/sample.bb"
    ))
    file_name_bw = os.path.realpath(os.path.join(
        os.path.dirname(__file__),
        "../tests/data/sample.bw"
    ))
    file_paths = [file_name_bb, file_name_bw]

    dm_handle = dmp(test=True)

    for file_path in file_paths:
        results = dm_handle.get_file_by_file_path(user, file_path, True)

        assert isinstance(results, type([])) is True
        assert len(results) == 1

        for result in results:
            assert 'file_path' not in result
