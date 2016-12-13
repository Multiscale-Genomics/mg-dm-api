import random

from dmp import dmp

users = ["adam", "ben", "chris", "denis", "eric"]
file_types = ["fastq", "fasta", "bam", "bed", "hdf5", "tsv", "wig", "pdb"]
data_types = ['RNA-seq', 'MNase-Seq', 'ChIP-seq', 'WGBS', 'HiC']

da = dmp()

for i in xrange(10):
    u = random.choice(users)
    ft = random.choice(file_types)
    results = da.get_files_by_file_type(u, ft)
    print len(results)
