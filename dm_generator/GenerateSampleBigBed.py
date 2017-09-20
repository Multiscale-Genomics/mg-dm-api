#!/usr/bin/env python

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

import subprocess
import shlex

from random import randint


class GenerateSampleBigBed(object):
    """
    Generate a sample adjacency if one does not exist
    """

    def main(self):
        """
        Main function
        """
        filename = "/tmp/sample.bed"
        bed_handle = open(filename, "w")
        start = 300000
        for i in range(500000):
            b_end = start + randint(20, 30)
            bed_handle.write("19\t" + str(start) + "\t" + str(b_end) + "\t" + "test." + str(i) + "\t0\t+\n")

            start = b_end + randint(50, 100)
        bed_handle.close()

        cs_file = "/tmp/chrom.size"
        cs_handle = open(cs_file, "w")
        cs_handle.write("19\t" + str(start + 1000) + "\n")
        cs_handle.close()

        command_line = "bedToBigBed /tmp/sample.bed /tmp/chrom.size /tmp/sample.bb"
        args = shlex.split(command_line)
        process_handle = subprocess.Popen(args)
        process_handle.wait()

if __name__ == '__main__':
    GSBB = GenerateSampleBigBed()
    GSBB.main()
