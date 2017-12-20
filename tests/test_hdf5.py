#!/usr/bin/python

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

import random
import os

from reader.hdf5_reader import hdf5_reader
import pytest # pylint: disable=unused-import

# 286sec ==> 0.286sec per query

def test_hdf5():
    """
    Test for retrieving lists of files that have features within a defined
    genomic range
    """
    cnf_loc = os.path.dirname(os.path.abspath(__file__)) + '/mongodb.cnf'

    chr_list = [
        1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10,
        11, 12, 13, 14, 15, 16, 17, 18, 19,
        'X', 'Y'
    ]
    #chr_list = [1,10,11,12,13]

    i = 0
    while i < 10:
        chromosome = random.choice(chr_list)
        start = random.randint(1, 45000000)
        end = start + 1000000
        h5r = hdf5_reader('test', cnf_loc)

        # Mouse
        #file_ids = h5r.get_regions('GCA_000001635.7', chromosome, start, end)
        file_ids = h5r.get_regions('GRCm38', chromosome, start, end)

        # Human
        #file_ids = h5r.get_regions('GCA_000001405.22', chromosome, start, end)

        print(str(chromosome), str(start), str(end), str(file_ids))
        h5r.close()

        i += 1
