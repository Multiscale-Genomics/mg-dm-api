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

from __future__ import print_function

import random

from dmp import dmp


def test_loading():
    """
    Test the loading of new files into the MongoDB
    """

    users = ["adam", "ben", "chris", "denis", "eric"]
    file_types = ["fastq", "fa", "fasta", "bam", "bed", "hdf5", "tsv", "wig", "pdb"]
    data_types = ['RNA-seq', 'MNase-Seq', 'ChIP-seq', 'WGBS', 'HiC']
    compressed = [None, 'gzip', 'zip']

    dm_handle = dmp(test=True)

    for i in range(10):
        user_id = random.choice(users)
        file_type = random.choice(file_types)
        data_type = random.choice(data_types)
        zipped = random.choice(compressed)
        file_loc = '/tmp/test/' + data_type + '/test_' + str(i) + '.' + file_type
        file_id = dm_handle.set_file(
            user_id, file_loc, 'file', file_type, 64000, None, data_type, 9606, zipped,
            meta_data={'assembly': 'GCA_0123456789'}
        )

        if data_type == 'RNA-seq' and file_type == 'fastq' and random.choice([0, 1]) == 1:
            file_loc = '/tmp/test/' + data_type + '/test_' + str(i) + '.bam'
            file_id_2 = dm_handle.set_file(
                user_id, file_loc, 'file', 'bam', 64000, None, data_type, 9606, None, [file_id],
                meta_data={'assembly': 'GCA_0123456789'}
            )
            print(file_id_2)

    for user_id in users:
        results = dm_handle.get_files_by_user(user_id)
        print(user_id, len(results))
