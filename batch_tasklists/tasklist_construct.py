#!/usr/bin/env python3

#script for creating the ILL matrix

import re
import csv
from pathlib import Path
import os

wikipi_repo_path = Path(os.getcwd()).parent
out_path_base = Path(os.getcwd()) / 'output'

# basic construction
# python3 ./mwdumptools/wikiq [input] -o ./output  -RPl [regex] -RP [(WP:whatever)]

# constructing giant regex task lists (uing the giant regex) 
giant_regex_files = os.listdir(wikipi_repo_path / 'regex' / 'giant-regexes')
print(giant_regex_files)

#assuming we are on the sohw/wikipi/wikipi_repo
#../../batch_jobs/wikipi/input
input_files = wikipi_repo_path.parent / 'batch_jobs' / 'wikipi' / 'input'
print(input_files)