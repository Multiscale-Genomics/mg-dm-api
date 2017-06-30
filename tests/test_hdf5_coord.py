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

import pytest

from reader.hdf5_coord import coord

def _get_region_ids(hdf5_handle, more_than_1=False):
    """
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
            

#@pytest.mark.underdeverlopment
def test_resolutions():
    """
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()

    results_count = len(results)
    
    assert results_count > 0

def test_set_resolution():
    """
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
    """
    hdf5_handle = coord('test', '')
    chromosomes = hdf5_handle.get_chromosomes()

    chromosomes_count = len(chromosomes)
    assert chromosomes_count == 0

def test_get_chromosome_01():
    """
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()

    hdf5_handle.set_resolution(int(results[0]))

    chromosomes = hdf5_handle.get_chromosomes()

    print('Chromosomes:', chromosomes)

    assert chromosomes > 0

def test_get_regions():
    """
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    results = _get_region_ids(hdf5_handle)
    
    print('Get Regions:', len(results))
    assert results is not None

def test_get_region_order():
    """
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    region_ids = _get_region_ids(hdf5_handle, True)

    print('Region Order:', region_ids)
    results = hdf5_handle.get_region_order(region_ids['chromosome'], region_ids['region_ids'][0])

    print('Region Order:', results)
    assert results is not None

def test_object_data():
    """
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    region_ids = _get_region_ids(hdf5_handle)

    results = hdf5_handle.get_object_data(region_ids['region_ids'][0])

    print('Object Data:', results)
    assert 'assembly' in results

def test_clusters():
    """
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    region_ids = _get_region_ids(hdf5_handle)

    results = hdf5_handle.get_clusters(region_ids['region_ids'][0])

    print('Clusters:', results)
    cluster_count = len(results)
    assert cluster_count == 4

def test_centroids():
    """
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    region_ids = _get_region_ids(hdf5_handle)

    results = hdf5_handle.get_centroids(region_ids['region_ids'][0])

    print('Centroids:', results)
    centroid_count = len(results)
    assert centroid_count == 5

def test_get_models():
    """
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    region_ids = _get_region_ids(hdf5_handle)

    results = hdf5_handle.get_models(region_ids['region_ids'][0])

    print('Models:', len(results))
    models_count = len(results)
    assert models_count == 1000

def test_get_model():
    """
    """
    hdf5_handle = coord('test', '')
    results = hdf5_handle.get_resolutions()
    hdf5_handle.set_resolution(int(results[0]))

    region_ids = _get_region_ids(hdf5_handle)

    models = hdf5_handle.get_models(region_ids['region_ids'][0])
    results = hdf5_handle.get_model(region_ids['region_ids'][0], models[0])

    print('Model:', results[0]['object'])
    assert 'object' in results[0]
