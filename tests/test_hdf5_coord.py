"""
.. Copyright 2017 EMBL-European Bioinformatics Institute

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

import pytest # pylint: disable=unused-import

from reader.hdf5_coord import coord

def get_region_ids(hdf5_handle, more_than_1=False):
    """
    Return a list of available regions from any chromosome without knowing if
    any exist. This function is required when testing as the sample generation
    script has an element of randomness. This function removes this randomness
    by identifying where there is data allowing the test functions to work.

    Parameters
    ----------
    hdf5_handle : coord
        An open handle to the reader.hdf5_coord object
    more_than_1 : bool
        If querying for a chromosome with more than one set of models over a
        defined region is required

    Returns
    dict
        chromsome : str
            ID of the Chromosome that the region matches to
        region_ids : list
            List of the region identifiers with the region
    """
    chromosomes = ['chr1', 'chr2', 'chr3', 'chr4', 'chr5', 'chr6', 'X']

    region_ids = None
    chromosome = -1
    while chromosome < len(chromosomes):
        chromosome += 1
        region_ids = hdf5_handle.get_regions(chromosomes[chromosome], 0, 300000000)
        if len(region_ids) > 1 and more_than_1 is True:
            return {
                'chromosome' : chromosomes[chromosome],
                'region_ids' : region_ids
            }

        if region_ids is not None and more_than_1 is False:
            return {
                'chromosome' : chromosomes[chromosome],
                'region_ids' : region_ids
            }

    return None


def test_resolutions():
    """
    Test to get a list of the available resolutions that models have been
    generated for
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()

    results_count = len(results)

    assert results_count > 0

def test_set_resolution():
    """
    Test to set the resolution and ensure that the value has been updated in the
    object
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()

    res_before = hdf5_handle.get_resolution()

    hdf5_handle.set_resolution(int(results[0]))

    res_after = hdf5_handle.get_resolution()

    assert res_before is None
    assert res_after == int(results[0])

def test_get_chromosome_00():
    """
    Test the list of chromosomes when no resolution has been specied is zero
    """
    hdf5_handle = coord('test', '')
    chromosomes = hdf5_handle.get_chromosomes()

    chromosomes_count = len(chromosomes)
    assert chromosomes_count == 0

def test_get_chromosome_01():
    """
    Test that the list of chromosomes has values when the resolution is
    specified
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()

    hdf5_handle.set_resolution(int(results[0]))

    chromosomes = hdf5_handle.get_chromosomes()

    print('Chromosomes:', chromosomes)

    assert chromosomes > 0

def test_get_regions():
    """
    Test that there are regions returned when querying a given chromosome with a
    start and end value
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    results = get_region_ids(hdf5_handle)

    print('Get Regions:', len(results))
    assert results is not None

def test_get_region_order():
    """
    Test the ordering of models given a chromosome, start and end parameter
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    region_ids = get_region_ids(hdf5_handle, True)

    print('Region Order:', region_ids)
    results = hdf5_handle.get_region_order(region_ids['chromosome'], region_ids['region_ids'][0])

    print('Region Order:', results)
    assert results is not None

def test_object_data():
    """
    Test getting the header data for the JSON object
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    region_ids = get_region_ids(hdf5_handle)

    results = hdf5_handle.get_object_data(region_ids['region_ids'][0])

    print('Object Data:', results)
    assert 'assembly' in results

def test_clusters():
    """
    Test getting the list of clusters
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    region_ids = get_region_ids(hdf5_handle)

    results = hdf5_handle.get_clusters(region_ids['region_ids'][0])

    print('Clusters:', results)
    cluster_count = len(results)
    assert cluster_count == 4

def test_centroids():
    """
    Test getting the list of centroids
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    region_ids = get_region_ids(hdf5_handle)

    results = hdf5_handle.get_centroids(region_ids['region_ids'][0])

    print('Centroids:', results)
    centroid_count = len(results)
    assert centroid_count == 5

def test_get_models():
    """
    Test getting the list of models for a given region
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    region_ids = get_region_ids(hdf5_handle)

    results = hdf5_handle.get_models(region_ids['region_ids'][0])

    print('Models:', len(results))
    models_count = len(results)
    assert models_count == 1000

def test_get_model():
    """
    Test getting a model for a given region and model ID
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    region_ids = get_region_ids(hdf5_handle)

    models = hdf5_handle.get_models(region_ids['region_ids'][0])
    results = hdf5_handle.get_model(region_ids['region_ids'][0], models[0])

    print('Model:', results[0]['object'])
    assert 'object' in results[0]
