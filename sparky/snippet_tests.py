import sys
# add pyspark to your python path e.g.
sys.path.append("/home/sohw/sparkstuff/spark/python/pyspark")
sys.path.append("/home/sohw/sparkstuff/spark/python/")

import glob
from pathlib import Path
import os
import csv

# iterating through the column names
wd = Path(os.getcwd())
columns_file_path = wd / 'frwiki_columns'

# wikipi/wikipi_repo/sparky/...
# wikipi/wikiq_runs/output_samples/... --> e.g. eswiki_sample.tsv, jawiki_sample.tsv, frwiki_sample.tsv
samples_folder = wd.parent.parent / 'wikiq_runs' / 'output_samples'
print(samples_folder.as_posix())

samples = ["eswiki_sample.tsv"]

for s in samples:
	s_path = samples_folder / s
	with open(s_path.as_posix()) as tsv:
		reader = csv.reader(tsv, delimiter='\t')
		for row in reader:
		

'''
n = 0
with open(columns_file_path.as_posix()) as tsv:
    reader = csv.reader(tsv, delimiter='\t')
    for row in reader:
        n += 1
        print(n)
        print(row[0])
        print(type(row[0]))
'''
