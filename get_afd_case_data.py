import sys
import os
import pandas as pd
import numpy as np
import ast
import datefinder
from bs4 import BeautifulSoup
from collections import Counter
import wikifunctions as wf
import datetime
import requests

pd.set_option('display.max_columns', None)

def get_page_numbers(row):
    """
    Returns the number of revisions, number of unique contributors, and a dictionary of revision counts mapped to users a page.
    """
    page_title = row['name']
    revs_df = wf.get_all_page_revisions(page_title)
    num_revs = len(revs_df)
    num_unique_users = len(set(revs_df.user.values.tolist()))
    user_contri_counter = Counter ( revs_df.user.values.tolist() )
    return num_revs, num_unique_users, user_contri_counter

def get_afd_discussion_content(row):
    page_title = row['name']
    soup = BeautifulSoup(wf.get_page_raw_content(page_title),features="lxml")
    box = soup.find_all("div", class_="mw-parser-output")[0]

    # extract page link and title
    a = box.find_all('h3')[0].find_all('a')[0]
    url = a.get('href')
    title = a.get('title')
    if '(page does not exist)' in title:
        page_exists = False
    else:
        page_exists = True
    return url, title, page_exists

def get_discussed_page_data(row):
    """
    For pages that do exist, we get page numbers (number of revisions, number of unique contributors, num) and the current categorizations of page quality. 
    """
    num_revs, num_unique_users, user_contri_counter = get_page_numbers(row)
    page = row["discussed_page_title"]

    html = requests.get("https://en.wikipedia.org/wiki/{}".format(page)).text
    soup = BeautifulSoup(html, 'html.parser')
    returned_name = soup.find_all("h1")[0].string

    # talk page categories incldue the quality and importance ratings
    cats = wf.get_category_memberships("Talk:{}".format(page))
    cats_class = [c.lower() for c in cats if ('class' in c.lower())]
    cats_import = [c.lower() for c in cats if ('importance' in c.lower())]
    if len(cats) == 0:
        return num_revs, num_unique_users, user_contri_counter, None, None

    return returned_name, num_revs, num_unique_users, user_contri_counter, cats_class, cats_import

def get_revisions_of_all_in_list(list_of_pages, is_talk):
    dfs = []
    for p in list_of_pages:
        if is_talk == True:
            p = "Talk:{}".format(p)
        print("\t{}".format(p))
        _temp = wf.get_all_page_revisions(p)
        dfs.append(_temp)
    return pd.concat(dfs)

# afd_cases_women.tsv is generated on afd.ipynb
cases_df = pd.read_csv('afd_cases_women.tsv',header=0,sep='\t')
# Comment out .head(..) when running actual thing

df = cases_df.copy()

print("> Getting some metrics for revisions to AfD pages.")
# get some numbers for summary stats and whatever
df[['num_revs', 'num_unique_users', 'user_contri_counter']] = df.apply(get_page_numbers, axis=1, result_type="expand")
df[['discussed_page_link', 'discussed_page_title', 'discussed_page_exists']] = df.apply(get_afd_discussion_content, axis=1, result_type="expand")
df.to_csv('afd_cases_women_afdpage_data.tsv',sep='\t',header=True,index=False)

print("> Getting some metrics for the pages that survived the AfD pages; note that some are merged, so it's technically a different page.")
# get some data for pages that still exist
discussed_pages_df = df.loc[df.discussed_page_exists==True].copy()
if len(discussed_pages_df) > 0:
    discussed_pages_df[['returned_name','num_revs', 'num_unique_users', 'user_contri_counter', 'cats_class', 'cats_import']] = discussed_pages_df.apply(get_discussed_page_data, axis=1, result_type="expand")
    discussed_pages_df.to_csv('afd_cases_women_discussedpage_data.tsv',sep='\t',header=True,index=False)

print("> Getting and saving all revisions for all of the afd pages.")
# collect all the revisions for each of the afd pages
cases = cases_df.name.values.tolist()
afd_revisions_df = get_revisions_of_all_in_list(cases, False)
afd_revisions_df.to_csv('afd_cases_women_afdpage_revisions_data.tsv',sep='\t',header=True,index=False)

print("> Getting and saving all revisions for all of the pages discussed on AfD that now/still exist.")
# collect all the revisions for the afd surviving pages
discussed_pages_revisions_df = get_revisions_of_all_in_list(list(set(discussed_pages_df.returned_name.values.tolist())), False)
discussed_pages_revisions_df.to_csv('afd_cases_women_discussedpage_revisions_data.tsv',sep='\t',header=True,index=False)

print("> Getting and saving all revisions for TALK pages of all of the pages discussed on AfD that now/still exist.")
# collect all the revisions for the afd surviving pages
discussed_pages_revisions_talk_df = get_revisions_of_all_in_list(list(set(discussed_pages_df.returned_name.values.tolist())), True)
discussed_pages_revisions_talk_df.to_csv('afd_cases_women_discussedpage_revisions_talk_data.tsv',sep='\t',header=True,index=False)