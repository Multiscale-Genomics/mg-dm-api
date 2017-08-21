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

import pyBigWig

from dmp import dmp


class bigbed_reader(object): # pylint: disable=invalid-name
    """
    Class related to handling the functions for interacting directly with the
    BigBed files. All required information should be passed to this class.
    """

    def __init__(self, file_path=''):
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
        """

        # Open the bigbed file
        self.f = pyBigWig.open(file_path, 'r')

    def close(self):
        """
        Tidy function to close file handles

        Example
        -------
        .. code-block:: python
           :linenos:

           from reader.bigbed import bigbed_reader
           bbr = bigbed_reader('test')
           bbr.close()
        """
        self.f.close()

    def get_chromosomes(self):
        """
        List the chromosome names and lengths

        Returns
        -------
        chromosomes : dict
            Key value pair of chromosome name and the value is the length of the
            chromosome.

        """

        return self.f.chroms()

    def get_header(self):
        """
        Get the bigBed header

        Returns
        -------
        header : dict
        """
        return self.f.header()

    def get_range(self, chr_id, start, end, file_type="bed"):
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
        file_type : string (OPTIONAL)
            `bed` format returning the whole file as a string is the default
            option. `list` will return the bed rows but as a list of lists.

        Returns
        -------
        bed : str (DEFAULT)
            List of strings for the rows in a bed file
        bed_array : list
            List of lists of each row for the bed file format

        """

        print("GET RANGE:", str(chr_id), int(start), int(end), file_type)
        try:
            bb_features = self.f.entries(str(chr_id), int(start), int(end))
        except RuntimeError:
            bb_features = []

        if bb_features is None:
            bb_features = []

        if file_type == "bed":
            bed_array = []
            for feature in bb_features:
                row = str(chr_id) + "\t" + str(feature[0]) + "\t" + str(feature[1]) + "\t" + feature[2]
                bed_array.append(row)

            return "\n".join(bed_array) + "\n"

        bed_array = []
        for feature in bb_features:
            row = [chr_id, feature[0], feature[1]] + feature[2].split("\t")
            bed_array.append(row)

        return bed_array
