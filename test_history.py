import random

from dmp import dmp

da = dmp()

history = da.get_file_history("random_file_id")

print history
