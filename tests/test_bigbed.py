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

from reader.bigbed import bigbed_reader
import pytest # pylint: disable=unused-import

def test_bigbed():
    """
    Testing the bigbed reader
    """

    chromosome = 19
    start = 3000000
    end = 3001000
    bbr = bigbed_reader('test', '/tmp/sample.bb')

    bed_str = bbr.get_range(chromosome, start, end, 'bed')
    assert isinstance(bed_str, str)
    assert len(bed_str) > 0

    bed_obj = bbr.get_range(chromosome, start, end, 'json')
    assert isinstance(bed_obj, list)
    assert len(bed_obj) > 0
