import random

from dmp import dmp

users = ["adam", "ben", "chris", "denis", "eric"]
file_types = ["fastq", "fa", "fasta", "bam", "bed", "hdf5", "tsv", "wig", "pdb"]
data_types = ['RNA-seq', 'MNase-Seq', 'ChIP-seq', 'WGBS', 'HiC']
compressed = [None, 'gzip', 'zip']

da = dmp(test=True)

for i in range(10):
    u = random.choice(users)
    ft = random.choice(file_types)
    dt = random.choice(data_types)
    z  = random.choice(compressed)
    f = '/tmp/test/' + dt + '/test_' + str(i) + '.' + ft
    file_id = da.set_file(u, f, ft, dt, 9606, z, meta_data={'assembly' : 'GCA_0123456789'})
    
    if dt == 'RNA-seq' and ft == 'fastq' and random.choice([0,1]) == 1:
         f = '/tmp/test/' + dt + '/test_' + str(i) + '.bam'
         da.set_file(u, f, 'bam', dt, 9606, None, [file_id], meta_data={'assembly' : 'GCA_0123456789'})

for u in users:
    results = da.get_files_by_user(u)
    print(u, len(results))
