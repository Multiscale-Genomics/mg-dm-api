Custom Reader APIs
==================

.. automodule:: reader

   HDF5 Files
   ==========

   Hi-C Adjacency Files
   --------------------
   .. autoclass:: reader.hdf5_adjacency.adjacency
      :members:

   Hi-C Coordinate Files
   ---------------------
   .. autoclass:: reader.hdf5_coord.coord
      :members:

   Text File Index
   ---------------
   Lists all files that are available for a user in bed and wig formats and
   lists the files than have data in a given region so that only the required
   files are requested by the client

   .. autoclass:: reader.hdf5_reader.hdf5_reader
      :members:


   BigBed Files
   ============
   .. autoclass:: reader.bigbed.bigbed_reader
      :members:

   BigWig Files
   ============
   .. autoclass:: reader.bigwig.bigwig_reader
      :members:

   Tabix Files
   ============
   .. autoclass:: reader.tabix.tabix
      :members:
