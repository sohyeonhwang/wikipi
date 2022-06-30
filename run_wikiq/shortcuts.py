#!/usr/bin/env python3

from bs4 import BeautifulSoup
import requests
import re
import csv
import pandas as pd
import argparse
import ast

def get_enwiki(rules):
    rows = {}
    for rule in rules:
        url = 'https://en.wikipedia.org/wiki/{}'.format(rule)
        #print(url)
        rule_page = requests.get(url).text
        soup = BeautifulSoup(rule_page, 'html.parser')

        temp = []
        #Wikipedia:PageTitle
        temp.append(soup.find("h1").string)
        mods = soup.find_all(class_="module-shortcutlist")

        for m in mods:
            shortcuts = m.find_next_sibling("ul")
            if pd.isna(shortcuts):
                print("for {}, check for a Short URL box.".format(rule))
            else:
                for s in shortcuts:
                    temp.append(s.string)
            
        top_box = soup.select("div.shortcutbox.noprint > div.plainlist > ul > li")
        for x in top_box:
            temp.append(x.string)

        temp = list(dict.fromkeys(temp))
        rows[rule] = temp
    return rows

def get_eswiki(rules):
    rows = {}
    for rule in rules:
        url = 'https://es.wikipedia.org/wiki/{}'.format(rule)
        print(url)
        rule_page = requests.get(url).text
        soup = BeautifulSoup(rule_page, 'html.parser')

        temp = []
        #Wikipedia:PageTitle
        # find the shortcuts
        temp.append(soup.find("h1").string)
        #go through the boxes with shortcuts
        links_box = soup.select("#shortcut > a.external.text")
        for sc in links_box:
            temp.append(sc.string)
        
        #remove any duplicates
        temp = list(dict.fromkeys(temp))
        rows[rule] = temp
    return rows

def get_frwiki(rules):
    rows = {}
    for rule in rules:
        url = 'https://fr.wikipedia.org/wiki/{}'.format(rule)
        print(url)
        rule_page = requests.get(url).text
        soup = BeautifulSoup(rule_page, 'html.parser')

        temp = []
        #Wikipedia:PageTitle
        # find the shortcuts
        temp.append(soup.find("h1").string)
        #go through the boxes with shortcuts
        links_box = soup.select("div.boite-raccourci.boite-grise.noprint a.mw-redirect")
        for sc in links_box:
            temp.append(sc.string)
        
        #remove any duplicates
        temp = list(dict.fromkeys(temp))
        rows[rule] = temp
    return rows

def get_jawiki(rules):
    rows = {}
    for rule in rules:
        url = 'https://ja.wikipedia.org/wiki/{}'.format(rule)
        print(url)
        rule_page = requests.get(url).text
        soup = BeautifulSoup(rule_page, 'html.parser')

        temp = []
        #Wikipedia:PageTitle
        # find the shortcuts
        temp.append(soup.find("h1").string)
        # find the top box with the shortcuts and extract the shortcut links
        sc_case0 = soup.select("#shortcut > a.external.text")

        #check rest of the page
        # div.shortcutbox.shortcutbox-policy.noprint > tbody > tr > th > small a.external.text
        # general and action policies
        sc_case1 = soup.select("table.shortcutbox.shortcutbox-policy.noprint > tbody > tr > th > small > span.plainlinks > a")
        
        # "div.module-shortcutboxplain.plainlist.noprint > ul > span.plainlinks > a"
        # content policies
        sc_case2 = soup.select("div.module-shortcutboxplain.plainlist.noprint > ul > li > span.plainlinks > a")

        shortcuts = sc_case0 + sc_case1 + sc_case2

        for x in shortcuts:
            temp.append(x.string)
        
        #remove any duplicates
        temp = list(dict.fromkeys(temp))
        rows[rule] = temp

        #print(rule)
        #print(temp)

    return rows

def clean_wikiprefix(shortcuts,lang):
    sans_partitions = []
    for s in shortcuts:
        partitioned = s.partition(":")[2:][0]
        sans_partitions.append(partitioned)
    v1 = []
    for p in sans_partitions:
        if lang == "fr":
            v1.append("Wikipédia:{}".format(p))
        else:
            v1.append("Wikipedia:{}".format(p))
    v2 = []
    for p in sans_partitions:
        v2.append("WP:{}".format(p))
    return v1+v2

def parse_args():
    parser = argparse.ArgumentParser(description='Create a dataset.')
    parser.add_argument('-i', '--input', help='', required=True, type=str)
    parser.add_argument('-o', '--outdir', help='Output directory', default='./', type=str)
    parser.add_argument('-l', '--lang', help='',required=True, type=str)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    args = parse_args()
    input_file_path = args.input
    lang = args.lang
    output_directory = args.outdir
    df = pd.read_csv(input_file_path,sep='\t',header=0)
    df = df.loc[df.lang == lang]
    rules = df.rule.values.tolist()
    print(rules)


    if lang == 'en':
        ss = get_enwiki(rules)
    elif lang == 'es':
        ss = get_eswiki(rules)
    elif lang == 'fr':
        ss = get_frwiki(rules)
    elif lang == 'ja':
        ss = get_jawiki(rules)
    
    # ss got manually stored in shortcuts
    '''
    shortcuts = df.shortcuts.values.tolist()
    shortcuts = [ast.literal_eval(x) for x in shortcuts]
    shortcuts = [clean_wikiprefix(x,lang) for x in shortcuts]

    i = 1
    for s in shortcuts:
        print("{}\tR{}\t{}\t{}\t".format(lang,i,s[0],s))
        i += 1
    '''