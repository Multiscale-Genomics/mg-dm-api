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

import os
import h5py
import numpy as np

from dmp import dmp


class hdf5_reader(object):  # pylint: disable=invalid-name
    """
    Class related to handling the functions for interacting directly with the
    HDF5 files. All required information should be passed to this class.
    """

    def __init__(self, user_id, file_id, cnf_loc=''):
        """
        Initialise the module and set default values

        Parameters
        ----------
        user_id : str
            Identifier to uniquely locate the users files. Can be set to
            "common" if the files can be shared between users
        file_id : str
            Location of the file in the file system

        Example
        -------
        .. code-block:: python
           :linenos:

           from hdf5_reader import hdf5_reader
           h5r = hdf5_reader('test')
        """

        self.test_file = '../tests/data/region_idx.hdf5'
        self.user_id = user_id

        # Open the hdf5 file
        if user_id == 'test':
            resource_path = os.path.join(os.path.dirname(__file__), self.test_file)
            self.file_handle = h5py.File(resource_path, "r")
        else:
            self.user_id = user_id
            dm_handle = dmp(cnf_loc)
            file_obj = dm_handle.get_file_by_id(user_id, file_id)
            self.file_handle = h5py.File(file_obj['file_path'], 'r')

    def close(self):
        """
        Tidy function to close file handles

        Example
        -------
        .. code-block:: python
           :linenos:

           from hdf5_reader import hdf5_reader
           h5r = hdf5_reader('test')
           h5r.close()
        """
        self.file_handle.close()

    def get_assemblies(self):
        """
        List all assemblies for which there are files that have been indexed

        Returns
        -------
        assembly : list
            List of assemblies in the index

        Example
        -------
        .. code-block:: python
           :linenos:

           from hdf5_reader import hdf5_reader
           h5r = hdf5_reader('test')
           h5r.assemblies()
        """
        return [asm for asm in self.file_handle if asm != 'meta']

    def get_chromosomes(self, assembly):
        """
        List all chromosomes that are covered by the index

        Parameters
        ----------
        assembly : str
            Genome assembly ID

        Returns
        -------
        chromosomes : list
            List of the chromosomes for a given assembly in the index

        Example
        -------
        .. code-block:: python
           :linenos:

           from hdf5_reader import hdf5_reader
           h5r = hdf5_reader('test')
           asm = h5r.assemblies()
           chr_list = h5r.get_chromosomes(asm[0])
        """
        grp = self.file_handle[assembly]
        cid = list(np.nonzero(grp['chromosomes']))
        return [grp['chromosomes'][i] for i in cid[0]]

    def get_files(self, assembly):
        """
        List all files for an assembly. If files are missing they can either get
        loaded or the search can be performed directly on the bigBed files

        Parameters
        ----------
        assembly : str
            Genome assembly ID

        Returns
        -------
        file_ids : list
            List of file ids for a given assembly in the index

        Example
        -------
        .. code-block:: python
           :linenos:

           from hdf5_reader import hdf5_reader
           h5r = hdf5_reader('test')
           asm = h5r.assemblies()
           file_list = h5r.get_files(asm[0])
        """
        grp = self.file_handle[assembly]
        fid1 = list(np.nonzero(grp['files'][0]))
        fid1k = list(np.nonzero(grp['files'][1]))

        return {
            1: [grp['files'][i] for i in fid1[0]],
            1000: [grp['files'][i] for i in fid1k[0]]
        }

    def get_regions(self, assembly, chromosome_id, start, end):
        """
        List files that have data in a given region.

        Parameters
        ----------
        assembly : str
            Genome assembly ID
        chromosome_id : str
            Chromosome names as listed by the get_files function
        start : int
            Start position for the region of interest
        end : int
            End position for the region of interest

        Returns
        -------
        file_ids : list
            List of the file_ids that have sequence features within the region
            of interest

        Example
        -------
        .. code-block:: python
           :linenos:

           from hdf5_reader import hdf5_reader
           h5r = hdf5_reader('test')
           asm = h5r.assemblies()
           file_list = h5r.get_chromosomes(asm[0], 1, 1000000, 1100000)
        """
        file_idx = self.get_files(assembly)
        chrom_idx = self.get_chromosomes(assembly)

        grp = self.file_handle[assembly]
        dset1 = grp['data1']
        dset1k = grp['data1k']

        chromosome = str(chromosome_id)
        start = int(start)
        end = int(end)

        #chr X file X position
        dnp1 = dset1[chrom_idx.index(chromosome), :, start:end]
        dnp1k = dset1k[chrom_idx.index(chromosome), :, start:end]
        f_idx = []
        dnp_size = len(dnp1k)
        for i in range(dnp_size):
            if np.any(dnp1[i]) is True:
                f_idx.append(file_idx[1][i])

        dnp_size = len(dnp1k)
        for i in range(dnp_size):
            if np.any(dnp1k[i]) is True:
                f_idx.append(file_idx[1000][i])
        return f_idx
