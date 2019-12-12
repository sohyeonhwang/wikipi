#!/usr/bin/env python3

# extract the URLs
# get the shortcuts from the box
# add to the TSV file

# each row:
# CAT   URL PAGETITLE   SHORTCUT1   SHORTCUT2   ... SHORTCUT#

from bs4 import BeautifulSoup
import requests
import re
import csv

# get the pages with lists of policies and guidelines
# japanese
ja_list_pg = requests.get("https://ja.wikipedia.org/wiki/Wikipedia:%E6%96%B9%E9%87%9D%E3%81%A8%E3%82%AC%E3%82%A4%E3%83%89%E3%83%A9%E3%82%A4%E3%83%B3%E3%81%AE%E4%B8%80%E8%A6%A7").text
# single page has more 
#https://ja.wikipedia.org/wiki/Wikipedia:%E6%96%B9%E9%87%9D%E3%81%A8%E3%82%AC%E3%82%A4%E3%83%89%E3%83%A9%E3%82%A4%E3%83%B3%E3%81%AE%E4%B8%80%E8%A6%A7
# cross checking

filename = "ja_policies-guidelines.tsv"

soup = BeautifulSoup(ja_list_pg, "html.parser")

# narrow down on the sections desired
section = soup.select("#mw-content-text > div.mw-parser-output > ul > li > a")
# print(section)

# get a list of all the links to the policy + guideline pages

#print(len(section))
#print(section)

# the list to fill
rows = []

i = 1

#for l in section:
#    print("{}: {}".format(i,l.get('href')))
#    i += 1


#for every link
# [:32] are policies
# [32:55] general, behavior, content guidelines
# [32:88] all guidelines

for l in section[:88]:
    print("{}: {}".format(i,l.get('href')))
    i += 1

    #print(l.string)
    #print(len(check))

    url = 'https://ja.wikipedia.org{}'.format(l.get('href'))
    cat = "NA"
    
    #go to policy page
    outlink_pg = requests.get(url).text
    minisoup = BeautifulSoup(outlink_pg, "html.parser")

    title = minisoup.find("h1").string
    print(title)

    temp = []
    temp.append(title)

    #TO DO
    # find the top box with the shortcuts and extract the shortcut links
    sc_case0 = minisoup.select("#shortcut > a.external.text")

    #check rest of the page
    # div.shortcutbox.shortcutbox-policy.noprint > tbody > tr > th > small a.external.text
    # general and action policies
    sc_case1 = minisoup.select("table.shortcutbox.shortcutbox-policy.noprint > tbody > tr > th > small > span.plainlinks > a")
    
    # "div.module-shortcutboxplain.plainlist.noprint > ul > span.plainlinks > a"
    # content policies
    sc_case2 = minisoup.select("div.module-shortcutboxplain.plainlist.noprint > ul > li > span.plainlinks > a")

    shortcuts = sc_case0 + sc_case1 + sc_case2

    for x in shortcuts:
        print(x.string)
        temp.append(x.string)

    # remove duplicates
    temp = list(dict.fromkeys(temp))

    #update the data structure
    row = [cat,url,title,temp]
    rows.append(row)


    #check if there is a sublist of ul:
    check = l.parent.select("ul > li > a")

    if len(check) != 0:
        for sublink in check:
            url = 'https://ja.wikipedia.org{}'.format(sublink.get('href'))

            if "Text_of_GNU" in url:
                continue
        
            print("{}: {}".format(i,sublink.get('href')))
            i += 1
            cat = "NA"
            
            #go to policy page
            outlink_pg = requests.get(url).text
            minisoup = BeautifulSoup(outlink_pg, "html.parser")

            title = minisoup.find("h1").string
            print(title)

            temp = []
            temp.append(title)

            #TO DO
            # find the top box with the shortcuts and extract the shortcut links
            sc_case0 = minisoup.select("#shortcut > a.external.text")

            #check rest of the page
            # div.shortcutbox.shortcutbox-policy.noprint > tbody > tr > th > small a.external.text
            # general and action policies
            sc_case1 = minisoup.select("table.shortcutbox.shortcutbox-policy.noprint > tbody > tr > th > small > span.plainlinks > a")
            
            # "div.module-shortcutboxplain.plainlist.noprint > ul > span.plainlinks > a"
            # content policies
            sc_case2 = minisoup.select("div.module-shortcutboxplain.plainlist.noprint > ul > li > span.plainlinks > a")

            shortcuts = sc_case0 + sc_case1 + sc_case2

            for x in shortcuts:
                print(x.string)
                temp.append(x.string)

            # remove duplicates
            temp = list(dict.fromkeys(temp))

            #update the data structure
            row = [cat,url,title,temp]
            rows.append(row)

extras = ["https://ja.wikipedia.org/wiki/Wikipedia:%E4%BA%94%E6%9C%AC%E3%81%AE%E6%9F%B1",
          "https://ja.wikipedia.org/wiki/Wikipedia:%E8%A8%98%E4%BA%8B%E5%90%8D%E3%81%AE%E4%BB%98%E3%81%91%E6%96%B9/%E6%97%A5%E6%9C%AC%E3%81%AE%E7%9A%87%E6%97%8F",
          "https://ja.wikipedia.org/wiki/Wikipedia:%E8%91%97%E4%BD%9C%E6%A8%A9/20080630%E8%BF%84",
          "https://ja.wikipedia.org/wiki/Wikipedia:%E3%82%A2%E3%82%AB%E3%82%A6%E3%83%B3%E3%83%88%E4%BD%9C%E6%88%90%E8%80%85",
          "https://ja.wikipedia.org/wiki/Wikipedia:IP%E3%83%96%E3%83%AD%E3%83%83%E3%82%AF%E9%81%A9%E7%94%A8%E9%99%A4%E5%A4%96"]

print(" ")
print("TEST TEST TEST TEST TEST")
print(" ")

for url in extras:
    cat = "NA"
    
    #go to policy page
    outlink_pg = requests.get(url).text
    minisoup = BeautifulSoup(outlink_pg, "html.parser")

    title = minisoup.find("h1").string
    print(title)

    temp = []
    temp.append(title)

    #TO DO
    # find the top box with the shortcuts and extract the shortcut links
    sc_case0 = minisoup.select("#shortcut > a.external.text")

    #check rest of the page
    # div.shortcutbox.shortcutbox-policy.noprint > tbody > tr > th > small a.external.text
    # general and action policies
    sc_case1 = minisoup.select("table.shortcutbox.shortcutbox-policy.noprint > tbody > tr > th > small > span.plainlinks > a")
    
    # "div.module-shortcutboxplain.plainlist.noprint > ul > span.plainlinks > a"
    # content policies
    sc_case2 = minisoup.select("div.module-shortcutboxplain.plainlist.noprint > ul > li > span.plainlinks > a")

    shortcuts = sc_case0 + sc_case1 + sc_case2

    for x in shortcuts:
        print(x.string)
        temp.append(x.string)

    # remove duplicates
    temp = list(dict.fromkeys(temp))

    #update the data structure
    row = [cat,url,title,temp]
    rows.append(row)



# generate the entries in the file
print("creating the tsv file now....")
with open(filename, 'w') as tsvfile:
    writer = csv.writer(tsvfile, delimiter='\t')
    writer.writerows(rows)
print(" ")
