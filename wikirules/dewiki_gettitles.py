#!/usr/bin/env python3

# extract the URLs
# get the shortcuts from the box
# add to the TSV file

# each row:
# CAT   URL PAGETITLE   SHORTCUT1   SHORTCUT2   ... SHORTCUT#

from bs4 import BeautifulSoup
import requests, re, os, csv
from pathlib import Path

import pandas as pd
import numpy as np

# get the pages with lists of policies and guidelines
# german

rules = Path(os.getcwd()).parent / "wiki_rules" / "dewiki_links.tsv"
rules_df = pd.read_csv(rules,sep="\t",header=None)
rules_df = rules_df.rename(columns={0: "links"})
pagelinks = rules_df["links"].tolist()

# output 

errors = {"https://de.wikipedia.org/wiki/Wikipedia:Was_Wikipedia_nicht_ist":"Was Wikipedia nicht ist"}

temp_dict = {}

for i in range(0,len(pagelinks)):
    url = pagelinks[i]

    p = requests.get(url).text
    minisoup = BeautifulSoup(p, 'html.parser')
    title = minisoup.find("h1").string
    # starts with Wikipedia, Portal, Benutzer, Vorlage
    if title == None:
        title = errors[url]

    """
    if title[0:10] == "Wikipedia:":
        title = title[10:]
    elif title[0:9] == "Benutzer":
        title = title[9:]
    elif title[0:8] == "Vorlage:":
        title = title[8:]
    elif title[0:7] == "Portal:":
        title = title[7:]
    """

    print(i, url[30:],'\t',title)
    temp_dict[i] = [url,title]

out_df = pd.DataFrame.from_dict(temp_dict, orient='index')

#print(out_df.head(n=5))

outpath = Path(os.getcwd()).parent / "wiki_rules" / "dewiki.tsv"
out_df.to_csv(outpath,sep='\t',header=False) 