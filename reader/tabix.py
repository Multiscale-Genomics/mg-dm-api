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

import os
import pysam
import pyBigWig

from dmp import dmp


class tabix(object):  # pylint: disable=invalid-name
    """
    Class related to handling the functions for interacting directly with the
    BigBed files. All required information should be passed to this class.
    """

    test_file_gz = '../sample.gff3.gz'
    test_file_tbi = '../sample.gff3.gz.tbi'

    def __init__(self, user_id, file_id, cnf_loc=''):
        """
        Initialise the module and

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users or 'test' for a
            dummy file
        file_id : str
            Location of the file in the file system
        resolution : int (Optional)
            Level of resolution. This is optional, but only the functions
            get_resolutions() and set_resolutions() can be called. Once the
            resolution has been set then all functions are callable.
        """

        # Open the bigwig file
        if user_id == 'test':
            self.file_handle = pysam.TabixFile(self.test_file_gz)
        else:
            dm_handle = dmp(cnf_loc)
            file_obj = dm_handle.get_file_by_id(user_id, file_id)
            self.file_handle = pyBigWig.open(file_obj['file_path'], 'r')

    def get_range(self, chr_id, start, end, file_type="gff3"):
        """
        Get entries in a given range

        Parameters
        ----------
        chr_id : str
            Chromosome name
        start : int
            Start of the region to query
        end : int
            End of the region to query
        format : string (OPTIONAL)
            `gff3` format returning the whole file as a string is the default
            option. `list` will return the gff3 rows but as a list of lists.

        Returns
        -------
        gff3 : str (DEFAULT)
            List of strings for the rows in a gff3 file
        wig_array : list
            List of each row for the gff3 file format

        """
        gff3_array = []
        for feature in self.file_handle.fetch(chr_id, start, end):
            gff3_array.append("\t".join(feature))

        if file_type == 'gff3':
            return "\n".join(gff3_array)

        return gff3_array
