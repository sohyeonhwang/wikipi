import sys
import pandas as pd
import argparse
import numpy as np
import glob, os, re, requests
from pathlib import Path
from datetime import datetime
import time
import traceback
import json
from bs4 import BeautifulSoup
import wikifunctions as wf

def get_interlanguage_links(page_title, endpoint='en.wikipedia.org/w/api.php', redirects=1, multicore_dict=None):
    """The function accepts a page_title and returns a dictionary containing 
    the title of the page in its other languages

    page_title - a string with the title of the page on Wikipedia
    endpoint - a string that points to the web address of the API.
        This defaults to the English Wikipedia endpoint: 'en.wikipedia.org/w/api.php'
        Changing the two letter language code will return a different language edition
        The Wikia endpoints are slightly different, e.g. 'starwars.wikia.com/api.php'
    redirects - 1 or 0 for whether to follow page redirects, defaults to 1

    Returns:
    langlink_dict - a dictionary keyed by lang codes and page title as values
    """

    #query_string = "https://{1}.wikipedia.org/w/api.php?action=query&format=json&prop=langlinks&formatversion=2&titles={0}&llprop=autonym|langname&lllimit=500".format(page_title,lang)
    query_url = "https://{0}".format(endpoint)
    query_params = {}
    query_params['action'] = 'query'
    query_params['prop'] = 'langlinks'
    query_params['titles'] = page_title
    query_params['redirects'] = redirects
    query_params['llprop'] = 'autonym|langname'
    query_params['lllimit'] = 500
    query_params['format'] = 'json'
    query_params['formatversion'] = 2
    json_response = requests.get(url=query_url,params=query_params).json()
    #print(json_response)

    interlanguage_link_dict = dict()
    start_lang = endpoint.split('.')[0]
    #print(json_response['query']['pages'][0])
    if 'title' in json_response['query']['pages'][0]:
        final_title = json_response['query']['pages'][0]['title']
        interlanguage_link_dict[start_lang] = final_title
    else:
        final_title = page_title
        interlanguage_link_dict[start_lang] = final_title

    if 'langlinks' in json_response['query']['pages'][0]:
        langlink_dict = json_response['query']['pages'][0]['langlinks']

        for d in langlink_dict:
            lang = d['lang']
            title = d['title']
            interlanguage_link_dict[lang] = title

    if multicore_dict is None:
        return {final_title:interlanguage_link_dict}
    else:
        multicore_dict[final_title] = interlanguage_link_dict

def get_ills_aslist(langlinks_dict):
    '''
    Takes in the langlinks dict and returns a dictionary that has page titles as keys and a list of ILL language codes as values.
    '''
    ill_list = dict()
    for pagekey in langlinks_dict.keys():
        _temp = list()
        for langkey in langlinks_dict[pagekey].keys():
            _temp.append(langkey)
        ill_list[pagekey] = _temp
    return ill_list

def get_addedcontent(pagetitle,revid,lang):
    pagetitle = pagetitle.replace(" ","_")
    url = "https://{}.wikipedia.org/w/index.php?title={}&diff=prev&oldid={}".format(lang,pagetitle,revid)
    #if lang == 'de':
    #    url = "https://{}.wikipedia.org/w/index.php?title={}&dir=prev&oldid={}".format(lang,pagetitle,revid)
    # if en,es,ja,fr, diff=prev. if de, dir=prev
    #print(url)

    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    revadds = soup.find_all("td", class_="diff-addedline")
    revadds = [ str(r) for r in revadds if "<div>" in str(r) ]

    if len(revadds) != 0:
        cleaned_revadds = list()

        for r in revadds:
            minisoup = BeautifulSoup(r,"html.parser")
            text = minisoup.get_text()
            #print(text)
            cleaned_revadds.append(text)
        return ' '.join(cleaned_revadds)

    # else, there are no additions in this revision
    else:
        return None

def process_languagelinksdates(lang,langlinks_list,langlinks_dict):
    # initialize output. test = ["Wikipedia:Richtlinien","Wikipedia:Grundprinzipien","Wikipedia:Was Wikipedia nicht ist"]
    ill_dates = dict()
    pageslist = list(langlinks_list[lang].keys())

    for page in pageslist:
        found_langs = list()
        ill_dates[page] = dict()

        _temprevs = wf.get_all_page_revisions(str(page),endpoint='{}.wikipedia.org/w/api.php'.format(lang))
        _revdf = pd.concat(_temprevs).sort_values(by='timestamp')
        _revdf['tuples'] = list(zip(_revdf.timestamp, _revdf.revid))
        _tuples = _revdf['tuples'].tolist()

        # the interlanguage links that this page has
        _ills = langlinks_list[lang][page]
        if lang in _ills:
            _ills.remove(lang)
        otherlangs = ['de','en','fr','es','ja']
        otherlangs.remove(lang)

        # ill subset should the subset of ills that this page has that are in the langs we care about
        #_ills_full = ['{}:{}'.format(key,value) for (key,value) in langlinks_dict[lang][page].items()]
        _ills_subset = ['{}:{}'.format(key,value) for (key,value) in langlinks_dict[lang][page].items() if key in otherlangs]
        _ills_subset = set(_ills_subset)

        print(page, len(_tuples),_revdf.shape)
        print(_ills)

        pattern = re.compile(r"\[\[[^\]\d]+?\:.+?\]\]")

        for revision in _tuples:
            currentilldateswehave = set(list(ill_dates[page].keys()))
            if _ills_subset.issubset(currentilldateswehave):
                break

            timestamp,revid = revision[0],revision[1]

            bit = get_addedcontent(page,revid,lang)
            if bit == None:
                continue

            finds = re.findall(pattern,bit)
            if len(finds) == 0:
                continue
            else:
                for f in finds:
                    # change _ills to a set of langs of interest
                    if f[2:4] in _ills:
                        if f[2:-2] not in ill_dates[page].keys():
                            ill_dates[page][f[2:-2]] = timestamp.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S %z')
                            found_langs.append(f[2:4])
                        else:
                            continue
                    else:
                        continue

    return ill_dates, 0

def parse_args():
    parser = argparse.ArgumentParser(description='Script to get information about a page\'s interlanguage links.')
    parser.add_argument('-l', '--lang', help='language edition to do this for', default='2', type=str)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    # inputs
    args = parse_args()

    # load the appropriate list of rules
    rules_df = pd.read_csv(Path(os.getcwd()) / "{}wiki.tsv".format(args.lang),sep="\t",header=None)
    rules_df = rules_df.rename(columns={0: "NA", 1: "links",2:"titles"})

    # get the interlanguage links of each rule
    pagetitles = rules_df["titles"].tolist()
    interlanguagelinks = dict()

    endpoint = "{}.wikipedia.org/w/api.php".format(args.lang)
    temp = {}
    prev = ""
    for p in sorted(pagetitles):
        if p == prev:
            print("\n","Error! A repeat in rule pages to search for ILLs for: ",p)
        prev = p

        try:
            temp.update(get_interlanguage_links(p,endpoint))
        except KeyboardInterrupt:
            break
        except:
            print('!!! FAILED > {}'.format(p))
            pass

    # this has a dictionary { args.lang : {rulepage:{lang:name_on_lang,lang:name_on_lang}, rulepage:{lang:name_on_lang}, rulepage:{lang:name_on_lang} ...} }
    interlanguagelinks[args.lang] = temp

    # convert to a list --> {args.lang: {rulepage:[list of langs], rulepage:[list of langs], rulepage:[list of langs]}}
    ill_aslist = dict()
    ill_aslist[args.lang] = get_ills_aslist(interlanguagelinks[args.lang])

    illdates_data, completed = process_languagelinksdates('de',ill_aslist,interlanguagelinks)

    # output
    # if completed
    if completed == 0:
        with open('interlanguagelinks_dates_{}.json'.format(args.lang), 'w') as f:
            json.dump(illdates_data, f)
        print("\n\nDone collecting posts for this language edition: {}.".format(args.lang))
        print("Output at: interlanguagelinks_dates_{}.json".format(args.lang))

    else:
        with open('interlanguagelinks_dates_{}_INCOMPLETE.json'.format(args.lang), 'w') as f:
            json.dump(illdates_data, f)
        print("There was an error in collecting, so the output is INCOMPLETE.")
        print("Output at: interlanguagelinks_dates_{}.json".format(args.lang))
