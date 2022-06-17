from bs4 import BeautifulSoup
import requests
import re, os, csv
import wikifunctions_s as wfs
import pandas as pd
import json
import time

langs_10 = ['en','fr','de','ja','es','ru','zh','it','pt','fa']
print("Getting all pages in the Wikipedia Project namespace for {}".format(langs_10))

#'''
# get all pages in the Wikipedia namespace
rule_dfs_dict = {}
rule_dfs_list = []

for l in langs_10:
    print(l)
    _temp = wfs.get_all_pages_in_namespace(l)
    rule_dfs_dict[l] = _temp
    rule_dfs_list.append(_temp)

    print('{} has {} pages in the Wikipedia Project namespace'.format(l, len(rule_dfs_list)))

# export each df in rule_dfs_list so that we don't have to get them all over again
output_directory = 'nm4_all_list'

if not os.path.isdir(output_directory):
    os.mkdir(output_directory)

for df in rule_dfs_list:
    _lang = df.lang.values.tolist()[0]
    path = './{}/{}.tsv'.format(output_directory,_lang)
    df.to_csv(path,index=False,sep='\t')
#'''

# now load those dfs and get all the ILLs for the pages
print("Getting all ILLs for all pages in the Wikipedia Project namespace for {}".format(langs_10))
input("continue?")

rule_dfs_dict = {}
rule_dfs_list = []

output_directory = 'nm4_all_ills'

if not os.path.isdir(output_directory):
    os.mkdir(output_directory)

for l in langs_10:
    path = './nm4_all_list/{}.tsv'.format(l)
    _df = pd.read_csv(path, sep='\t',header=0)
    rule_dfs_dict[l] = _df
    rule_dfs_list.append(_df)

interlanguage_links = {}

for lang in langs_10:
    print(lang)
    start = time.time()
    interlanguage_links[lang] = {}
    # get the list of pages
    _df = rule_dfs_dict[lang]
    rule_list = _df['title'].values.tolist()

    for rule in rule_list[:100]:
        ill_dict = wfs.get_interlanguage_links(rule, endpoint=lang, redirects=1)
        interlanguage_links[lang][rule] = {}
        interlanguage_links[lang][rule]['count'] = len(list(ill_dict.keys()))
        interlanguage_links[lang][rule]['langs'] = list(ill_dict.keys())
        interlanguage_links[lang][rule]['links'] = ill_dict

    # export the interlanguage_links[lang]
    with open("./{}/ills_{}.json".format(output_directory,lang), "w") as outfile:
        json.dump(interlanguage_links[lang], outfile)
    
    end = time.time()
    print(end - start)

with open("./{}/ills_all.json".format(output_directory), "w") as outfile:
    json.dump(interlanguage_links, outfile)