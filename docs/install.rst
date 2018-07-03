Requirements and Installation
=============================

Requirements
------------

Software
^^^^^^^^

- Mongo DB 3.2
- Python 2.7.10+

Python Modules
^^^^^^^^^^^^^^

- pymongo
- mongomock
- h5py
- numpy
- pyBigWig
- pysam


Installation
------------

Directly from GitHub:

.. code-block:: none
   :linenos:

   git clone https://github.com/Multiscale-Genomics/mg-dm-api.git

Using pip:

.. code-block:: none
   :linenos:

   pip install git+https://github.com/Multiscale-Genomics/mg-dm-api.git


Documentation
-------------

To build the documentation:

.. code-block:: none
   :linenos:

   pip install Sphinx
   pip install sphinx-autobuild
   cd docs
   make html
