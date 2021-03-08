#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Wikipedia data scraping

This contains a script to scrape the revision histories of a list of rule pages for language edition X.

We depend substantially from code generously shared by Brian Keegan (bkeegan@gmail.com)
There are a variety of functions primarily for accessing the MediaWiki API 
to extract data page revisions, user revisions, article hyperlinks, category membership, and pageview dynamics.
These scripts invoke several non-standard libraries:

* WikiTools - https://code.google.com/p/python-wikitools/
* NetworkX - http://networkx.github.io/
* Pandas - http://pandas.pydata.org/

This code was put together by Sohyeon Hwang (sohyeonhwang@u.northwestern.edu) in 2020.

Inputs a list of rule pages (tsv)
Outputs a series of revision history tsvs for each rule page
'''

import argparse
from datetime import datetime
import pandas as pd
import numpy as np
import time, os
from pathlib import Path

import wikifunctions as wf

def parse_args():
    parser = argparse.ArgumentParser(description='Create a dataset.')
    #parser.add_argument('-i', '--input', help='Path for directory of wikiq tsv outputs', required=True, type=str)
    parser.add_argument('--lang', help='Specify which language edition', default='en',type=str)
    parser.add_argument('-o', '--outdir', help='Output directory', default='./output_rulepagerevs', type=str)
    args = parser.parse_args()
    return(args)

def add_talk(title,lang):
    if lang == "en":
        # Wikipedia or Template
        add = ' talk:'
        if title[0:10] == "Wikipedia:":
            title = title[:9] + add + title[10:]
        elif title[0:9] == "Template:":
            title = title[:8] + add + title[9:]

    elif lang == "es":
        # Wikipedia, Usuario, Usuaria
        add = ' discusión:'
        if title[0:10] == "Wikipedia:":
            title = title[:9] + add + title[10:]
        elif title[0:8] == "Usuario:" or title[0:8] == "Usuaria:":
            title = title[:7] + add + title[8:]

    elif lang == "ja":
        # Wikipedia, プロジェクト <-- project
        add = '‐ノート:'
        if title[0:10] == "Wikipedia:":
            title = title[:9] + add + title[10:]
        elif title[0:7] == "プロジェクト:":
            title = title[:6] + add + title[7:]

    elif lang == "fr":
        # Aide, Projet, Utilisateur, Wikipédia
        add = 'Discussion '
        if title[0:12] == "Utilisateur:":
            title = add + title
        elif title[0:10] == "Wikipédia:":
            title = add + title
        elif title[0:7] == "Projet:":
            title = add + title
        elif title[0:5] == "Aide:":
            title = add + title

    elif lang == "de":
        add = ' Diskussion:'
        if title[0:10] == "Wikipedia:":
            title = title[:9] + add + title[10:]
        elif title[0:9] == "Benutzer:":
            title = title[:8] + add + title[9:]
        elif title[0:8] == "Vorlage:":
            title = title[:7] + add + title[8:]
        elif title[0:7] == "Portal:":
            title = title[:6] + add + title[7:]
    
    else:
        print("Lang error in add_talk()")

    return title

if __name__ == "__main__":
    args = parse_args()
    # check inputs
    endpoint = "{}.wikipedia.org/w/api.php".format(args.lang)
    timestamp = datetime.utcnow().strftime("%Y-%m-%d") #_%H-%M-%S
    print("Looking at language edition: {} --> {}\n ".format(args.lang,endpoint))
    
    if not os.path.isdir("./{}".format(args.outdir)):
        os.mkdir("./{}".format(args.outdir))
    if not os.path.isdir("./{}/{}".format(args.outdir,timestamp)):
        os.mkdir("./{}/{}".format(args.outdir,timestamp))

    outdir = "./{}/{}".format(args.outdir,timestamp)

    rulepages = Path(os.getcwd()) / "{}wiki.tsv".format(args.lang)
    rulepages_df = pd.read_csv(rulepages,sep="\t",header=None)

    if args.lang == "de":
        rulepages_df = rulepages_df.rename(columns={0: "i", 1: "links",2:"titles"})
        rulepages_df.drop(labels=["i"],axis=1,inplace=True)
    else:
        rulepages_df = rulepages_df.rename(columns={0: "NA", 1: "links", 2: "titles", 3:"shortcuts"})
        rulepages_df.drop(labels=["NA","shortcuts"],axis=1,inplace=True)
    #print(rulepages_df.head(n=1))

    pagelinks = rulepages_df["links"].tolist()
    pagetitles = rulepages_df["titles"].tolist()

    # UNCOMMENT SECTION TO GET REVISIONS
    print("Getting rule page revisions...")
    temp = {}
    prev = ""
    print(len(pagetitles))
    for p in sorted(pagetitles):
        print(p)
        if p == prev:
            print("\n","Error! A repeat: ",p)
        prev = p
        # get_all_page_revisions(page_title, endpoint='en.wikipedia.org/w/api.php', redirects=1, multicore_dict=None):
        try:
            temp.update(wf.get_all_page_revisions(p,endpoint=endpoint))
        except KeyboardInterrupt:
            break
        except:
            print('!!! FAILED > {}'.format(p))
            pass

    revdf = pd.concat(temp)
    print("There are {0:,} revisions for {1:,} articles in the concatenated DataFrame.".format(len(revdf),len(revdf['page'].unique())))

    output_path = Path(os.getcwd()) / outdir / "{}_revisions.tsv".format(args.lang)
    print(output_path)
    revdf.to_csv(output_path,sep='\t',encoding='utf8',index=False)

    print("Getting rule talk page revisions...")
    ruletalkpages = [add_talk(i,args.lang) for i in pagetitles]
    
    prev = ""
    pd.set_option("display.max_columns", 2)
    temp = {}
    for p in sorted(ruletalkpages):
        #print(p)
        if p == prev:
            print("\n","Error! A repeat: ",p)
        prev = p
        try:
            temp.update(wf.get_all_page_revisions(p,endpoint=endpoint))
        except KeyboardInterrupt:
            break
        except:
            print('!!! Failed (no talk page?) > {}'.format(p))
            pass

    rev_talk_df = pd.concat(temp)
    print("There are {0:,} revisions for {1:,} articles in the concatenated DataFrame.".format(len(rev_talk_df),len(rev_talk_df['page'].unique())))

    output_path = Path(os.getcwd()) / outdir / "{}_revisions_talk.tsv".format(args.lang)
    print(output_path)
    rev_talk_df.to_csv(output_path,sep='\t',encoding='utf8',index=False)

    print("Done.")
