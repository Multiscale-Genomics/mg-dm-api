#!/usr/bin/python

"""
Copyright 2017 EMBL-European Bioinformatics Institute

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

#import pyBigWig
from reader.bigbed import bigbed_reader
import random
import pytest

#bb = pyBigWig.open("DRR000386.sorted.filtered.bigBed", "r")
#chrom = list(bb.chroms().keys())
#e = bb.entries('X', 10500000, 11436650)
#out_of_range = 0
#for i in e:
#    if i[0] < 10000000 or i[1] > 11436650:
#        print("X\t" + str(i[0]) + "\t" + str(i[1]) + "\t" + i[2])
#        out_of_range += 1
#bb.close()
#print(str(out_of_range) + " out of " + str(len(e)))

@pytest.mark.underdeverlopment
def test_bigbed():
    """

    """
    
    chr_list = [19]

    for i in range(10):
        chromosome = random.choice(chr_list)
        start = random.randint(1,45000000)
        end = start + 1000000
        bbr = bigbed_reader('test')
        
        # Mouse
        #file_ids = h5r.get_regions('GCA_000001635.7', chromosome, start, end)
        file_ids = bbr.get_range(chromosome, start, end)
        
        # Human
        #file_ids = h5r.get_regions('GCA_000001405.22', chromosome, start, end)
        
        #print(str(chromosome), str(start), str(end), str(file_ids))
        bbr.close()

    #for i in range(10):
    #    chromosome = random.choice(chr_list)
    #    start = random.randint(1,60000000)
    #    end = start + 100000
    #    fid = []
    #    for i in range(10):
    #        bb = pyBigWig.open("DRR000386.bb", "r")
    #        try:
    #            e = bb.entries(str(chromosome), start, end)
    #            if e != None and len(e) > 0:
    #                fid.append("DRR000386.bed")
    #        except:
    #            print('No results')
    #        bb.close()
    #    #output = ""
    #    #for i in e:
    #    #    output += "X\t" + str(i[0]) + "\t" + str(i[1]) + "\t" + i[2] + "\n"
    
    
