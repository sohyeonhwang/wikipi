#!/usr/bin/env python3

#script for creating the ILL matrix

from bs4 import BeautifulSoup
import requests
import re
import csv

# for each language edition: es, fr, ja

# for each policy page, we want (1) outlinks in the content and (2) ILLs

'''
EXAMPLE:
es --> {policyname:[url,ILL,outlinks], policyname:[url,ILL,outlinks]} 
    policyname
        url = corresponding_url
        ILL = {fr:fr_ill_url,ja:ja_ill_url}
        outlinks = [url,url,url...]

NETWORKX
ILL
es.policy.url --> fr_ill_url --> fr.policy.url
              --> ja_ill_url --> ja.policy.url

for each language edition:
    for each policy:
        node for that policy
        for each ill_url:
            node for that lang edition (policy.url)
            directed from es -> lang

this should result in one big network

> check for one-directional links

> % of policies that ILL to the other ones is likely to be pretty high

% of lang1's rules that ILL to lang2
    es = {"fr":0.95, "ja":0.70}
    fr = {"es":0.80, "ja":0.50}
    ja = {"fr":0.89, "es":0.76}

% of lang1's rules that lang1 and lang2 share (mutually point to one another).
    if es.policy.url --> fr_ill_url = fr.policy.url --> es_ill_url = es.policy.url

                    2
          es    | fr    | ja    
     es | ----  | 0.80  | 0.50
 1   fr | 0.80  | ----  | 0.70
     ja | 0.50  | 0.70  | ----



OUTLINKS
we also do this network generation (separately) for OUTLINKS. each language edition will have it's own network, of course.

START WITH the rules on /FIVE PILLARS/

es.policy --> outlink1.es --[ ill ]-- outlink1.fr <---- fr.policy
this indicates an overlap in conceptual alignment/placement...




'''