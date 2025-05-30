import requests
import re, os, csv, glob
import pandas as pd
import time
import json
from collections import Counter
from pathlib import Path

langs = ['de','en','es','fr','ja']

project_dir = Path.cwd().parent.parent
print(project_dir)
raw_data = project_dir / 'raw_data'

selected_rules_list = ["R12", "R2", "R3"] #NPOV is R3. Notability is R12. IAR is R2
selected_rule = "_".join(selected_rules_list)

regexes = pd.read_csv('rule_regex_generated.tsv',sep='\t',header=0) #_wide
regexes = regexes.loc[regexes.label.isin(selected_rules_list)]

all_calls = []

for lang in langs:
    calls = []

    print(lang)
    lang_data_dir = raw_data / '{}wiki'.format(lang)
    dumps = glob.glob('{}/*'.format(lang_data_dir.as_posix()))

    print(lang_data_dir, len(dumps))

    # construct the giant regex -RP and -RPl pairs
    regex_statement = ''
    lang_regexes = regexes.loc[regexes.lang==lang]

    pattern_pairs = list(zip(lang_regexes.label,lang_regexes.regex))
    print(len(pattern_pairs))
    for p in pattern_pairs:
        pattern = '-RP "{}" -RPl "{}" '.format(p[1].replace(' ','\s'),p[0]) 
        regex_statement = '{} {}'.format(regex_statement,pattern)
    print(regex_statement)

    # for each dump
    for dump in dumps:
        call = '-u -o /gscratch/comdata/raw_data/sohw_wikiq_outputs_202302/{} -n 1 {}{}'.format(
            selected_rule,
            dump,
            regex_statement
        )
    
        calls.append(call)
        all_calls.append(call)
    
    # make the lang-specific task list
    with open("tasklist_{}_{}.sh".format(selected_rule,lang),"w") as f:
        for c in calls:
            f.write("{}\n".format(c))

# make the overall task list
with open('tasklist_{}.sh'.format(selected_rule),'w') as f:
    for c in all_calls:
        f.write('{}\n'.format(c))