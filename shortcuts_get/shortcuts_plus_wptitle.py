#!/usr/bin/env python3

import re
import csv
from pathlib import Path
import os
from bs4 import BeautifulSoup
import requests

wikipi_repo_path = Path(os.getcwd()).parent
out_path_base = Path(os.getcwd()) / 'shortcuts_plus'

regex_lists_path = wikipi_repo_path / 'regex' / 'regex-lists'
shortcuts_output_path = wikipi_repo_path / 'shortcuts_get' / 'output'

#input files --> the shortcuts
#FR - fr_policies-guidelines.tsv
#EN - en_policies-guidelines.tsv
#ES - es_policies-guidelines.tsv

input_fn = "en_policies-guidelines.tsv"
input_path = shortcuts_output_path / input_fn

print("Input: {}".format(input_path))

#output --> change lang ed prefix accordingly
output_fn = "en_all_plus.tsv"
output_path = shortcuts_output_path / output_fn
output_file = open(output_path.as_posix(), 'w', encoding='utf-8')

print("Output: {}".format(output_path))

with open(input_path.as_posix()) as tsv:
    reader = csv.reader(tsv, delimiter='\t')
    for row in reader:
        # get the url
        url = row[1]

        # get the title
        soup = BeautifulSoup(requests.get(url).text, 'html.parser')
        title = soup.find(id='firstHeading').string

        cat = "NA"

        
        if "Wikipédia" in title or 'Wikipedia' in title:
            plus = 'WP:{}'.format(title[10:])
            plused = "{}, '{}']".format(row[3][:-1],plus)
            print(title)
            row[3] = plused
            line = "{}\t{}\t{}\t{}\n".format(cat,url,title,plused)
            output_file.write(line)
        else:
            print(title)
            line = "{}\t{}\t{}\t{}\n".format(cat,url,title,row[3])
            output_file.write(line)