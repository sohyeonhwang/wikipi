import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import unquote, quote
from copy import deepcopy
import requests, re

def response_to_pages(json_response):
    pages = json_response['query']['allpages']
    pages = [p['title'] for p in pages]
    return pages

def get_all_pages_in_namespace(lang='en',apm=4):
    # store all pages here
    page_list = list()

    # query set-up
    query_url = "https://{}.wikipedia.org/w/api.php".format(lang)
    query_params = {}
    query_params['action'] = 'query'
    query_params['format'] = 'json'
    query_params['list'] = 'allpages'
    query_params['apnamespace'] = apm
    #query_params['apfilterlanglinks'] = 'withlanglinks'
    query_params['aplimit'] = 500
    query_params['apdir'] = 'ascending'
    
    # Make the query
    json_response = requests.get(url = query_url, params = query_params).json()

    # Add to a temp list of pages
    page_list += response_to_pages(json_response)

    # Loop for the rest of the pages
    while True:
        # Newer versions of the API return paginated results this way
        if 'continue' in json_response:
            query_continue_params = deepcopy(query_params)
            query_continue_params['apcontinue'] = json_response['continue']['apcontinue']
            json_response = requests.get(url = query_url, params = query_continue_params).json()
            page_list += response_to_pages(json_response)

        # If there are no more pages, stop
        else:
            break

    # Convert to a DataFrame
    df = pd.DataFrame(page_list,columns=['title'])
    df['lang'] = lang
    
    return df

def get_interlanguage_links(page_title, endpoint='en', redirects=1):
    """
    From: https://github.com/brianckeegan/wikifunctions

    The function accepts a page_title and returns a dictionary containing 
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
    query_url = "https://{0}.wikipedia.org/w/api.php".format(endpoint)
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
    
    interlanguage_link_dict = dict()
    start_lang = endpoint.split('.')[0]
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
            
    return interlanguage_link_dict