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
# ENGLISH
en_pol_list_pg = requests.get("https://en.wikipedia.org/wiki/Wikipedia:List_of_policies").text
en_gl_list_pg = requests.get("https://en.wikipedia.org/wiki/Wikipedia:List_of_guidelines").text

list_pages = [en_gl_list_pg,en_pol_list_pg]
list_pages_names = ["en_guidelines","en_policies"]

for i in range(0,len(list_pages)):
    p = list_pages[i]
    
    #create a new tsv file
    filename = '{}.tsv'.format(list_pages_names[i])
    print(filename)

    #create the html soup to work with
    soup = BeautifulSoup(p, 'html.parser')

    #narrow down
    section = soup.find_all(class_="mw-parser-output")

    #specify relevant headers
    headers = soup.find_all("h2")
    # get rid of headers we don't want or need
    headers = headers[1:-3] 
    header_titles = []
    for h in headers:
        for string in h.contents[0].strings:
            header_titles.append(string)

    #get the actual policy/guideline infos
    dts = soup.select("dl dt")
    #length of dts is 63


    # where we will put the rows to store
    rows = []

    #for each found policy/guideline
    for dt in dts:
        header = dt.find_parent("dl").find_previous_sibling("h2")
        cat = ""
        for x in header.contents[0].strings:
            cat = x

        # we grab the URL if it's in a relevant header
        if cat in header_titles:
            #get the url
            # exceptions in formatting re: guidelines page
            exc = ["Miscellaneous content guidelines","Categorization guidelines","Miscellaneous editing guidelines","Miscellaneous naming conventions",
            "Other notability guidelines","Other formatting and layout style guides","Style guidelines for images","List style guidelines","Other content style guidelines",
            "Arts","Music","Legal","Regional","Religion","Science","Sports"]

            if dt.string in exc:
                print(dt.string)
                a_tags = dt.find_next_sibling("dd").select("a")
                for a in a_tags:
                    url = 'https://en.wikipedia.org{}'.format(a.get('href'))

                    #get the title
                    title = a.string
                    
                    #get the shortcuts
                    #access the policy/guideline page
                    outlink_pg = requests.get(url).text
                    minisoup = BeautifulSoup(outlink_pg, 'html.parser')

                    temp = []
                    #Wikipedia:title of page
                    temp.append(minisoup.find("h1").string)

                    #shortcuts
                    mods = minisoup.find_all(class_="module-shortcutlist")
                    
                    for m in mods:
                        shortcuts = m.find_next_sibling("ul")
                        for s in shortcuts:
                            temp.append(s.string)
                    

                    #if this is the guideline page, want to double-check the top box as well
                    if 'guidelines' in filename:
                        top_box = minisoup.select("div.shortcutbox.noprint > div.plainlist > ul > li")
                        #print(test)
                        for x in top_box:
                            temp.append(x.string)
                            #print(x.string)

                    #remove any duplicates from temp
                    temp = list(dict.fromkeys(temp))
                
                    row = [cat,url,title,temp]
                    rows.append(row)

                    print(title)
                    print(len(temp), ": {}...".format(temp[:5]))
                    print(" ")

            else:
                url = 'https://en.wikipedia.org{}'.format(dt.a.get('href'))
            
                #get the title
                title = dt.a.string
                
                #get the shortcuts
                #access the policy/guideline page
                outlink_pg = requests.get(url).text
                minisoup = BeautifulSoup(outlink_pg, 'html.parser')

                temp = []
                #Wikipedia:title of page
                temp.append(minisoup.find("h1").string)

                #shortcuts
                mods = minisoup.find_all(class_="module-shortcutlist")
                
                for m in mods:
                    shortcuts = m.find_next_sibling("ul")
                    for s in shortcuts:
                        temp.append(s.string)
                

                #if this is the guideline page, want to double-check the top box as well
                if 'guidelines' in filename:
                    top_box = minisoup.select("div.shortcutbox.noprint > div.plainlist > ul > li")
                    #print(test)
                    for x in top_box:
                        temp.append(x.string)
                        #print(x.string)

                #remove any duplicates from temp
                temp = list(dict.fromkeys(temp))
            
                row = [cat,url,title,temp]
                rows.append(row)

                print(title)
                print(len(temp), ": {}...".format(temp[:5]))
                print(" ")

        #print(cat)
        #print(url)
        #print(title)
        #print(len(temp), ": {}...".format(temp[:5]))
        #print(" ")
    
    # add  to guidelines file
    if "guideline" in filename:
        cross_checked = ["https://en.wikipedia.org/wiki/Wikipedia:Scientific_citation_guidelines", 
                        "https://en.wikipedia.org/wiki/Wikipedia:Artist%27s_impressions_of_astronomical_objects",
                        "https://en.wikipedia.org/wiki/Wikipedia:In_the_news/Recurring_items", 
                        "https://en.wikipedia.org/wiki/Wikipedia:Identifying_reliable_sources_(medicine)",
                        "https://en.wikipedia.org/wiki/Wikipedia:Indic_transliteration",
                        "https://en.wikipedia.org/wiki/Wikipedia:Non-free_use_rationale_guideline",
                        "https://en.wikipedia.org/wiki/Wikipedia:Public_domain",
                        "https://en.wikipedia.org/wiki/Wikipedia:WikiProject_Economics/Reliable_sources_and_weight",
                        "https://en.wikipedia.org/wiki/Wikipedia:Shortcut",
                        "https://en.wikipedia.org/wiki/Wikipedia:Project_namespace",
                        "https://en.wikipedia.org/wiki/Wikipedia:Reference_desk/Guidelines/Medical_advice",
                        "https://en.wikipedia.org/wiki/Wikipedia:Talk_page_templates",
                        "https://en.wikipedia.org/wiki/Wikipedia:Userboxes",
                        "https://en.wikipedia.org/wiki/Wikipedia:Template_namespace",
                        "https://en.wikipedia.org/wiki/Wikipedia:Miscellany_for_deletion/Speedy_redirect",
                        "https://en.wikipedia.org/wiki/Wikipedia:As_of",
                        "https://en.wikipedia.org/wiki/Wikipedia:Extended_image_syntax",
                        "https://en.wikipedia.org/wiki/Wikipedia:Disambiguation/PrimaryTopicDefinition",
                        "https://en.wikipedia.org/wiki/Wikipedia:Overcategorization/User_categories",
                        "https://en.wikipedia.org/wiki/Wikipedia:Spellchecking",
                        "https://en.wikipedia.org/wiki/Wikipedia:TemplateStyles",
                        "https://en.wikipedia.org/wiki/Wikipedia:Template_namespace",
                        "https://en.wikipedia.org/wiki/Wikipedia:Wikimedia_sister_projects",
                        "https://en.wikipedia.org/wiki/Wikipedia:WikiProject_Council/Guide",
                        "https://en.wikipedia.org/wiki/Wikipedia:Manual_of_Style/Hidden_text",
                        "https://en.wikipedia.org/wiki/Wikipedia:Manual_of_Style/Military_history",
                        "https://en.wikipedia.org/wiki/Wikipedia:Manual_of_Style/Indonesia-related_articles",
                        "https://en.wikipedia.org/wiki/Wikipedia:Manual_of_Style/Pakistan-related_articles",
                        "https://en.wikipedia.org/wiki/Wikipedia:Manual_of_Style/Blazon",
                        "https://en.wikipedia.org/wiki/Wikipedia:Edit_filter",
                        "https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(US_stations)",
                        "https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(Irish_stations)",
                        "https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(Canadian_stations)",
                        "https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(places_in_Bangladesh)",
                        "https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(Football_in_Australia)",
                        "https://en.wikipedia.org/wiki/Wikipedia:Five_pillars]"]
        for url in cross_checked:
            #get the shortcuts
            #access the policy/guideline page
            outlink_pg = requests.get(url).text
            minisoup = BeautifulSoup(outlink_pg, 'html.parser')

            temp = []
            #Wikipedia:title of page
            temp.append(minisoup.find("h1").string)
            title = minisoup.find("h1").string

            #shortcuts
            mods = minisoup.find_all(class_="module-shortcutlist")
                
            for m in mods:
                shortcuts = m.find_next_sibling("ul")
                for s in shortcuts:
                    temp.append(s.string)
                
            #if this is the guideline page, want to double-check the top box as well
            top_box = minisoup.select("div.shortcutbox.noprint > div.plainlist > ul > li")
                #print(test)
            for x in top_box:
                temp.append(x.string)

            #remove any duplicates from temp
            temp = list(dict.fromkeys(temp))
            print(title)
            row = [cat,url,title,temp]
            rows.append(row)

    #generate the entries
    print("creating the tsv file now....")
    with open(filename, 'w') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')
        writer.writerows(rows)
    print(" ")

# create a log for what's being done.
#filelog = open("filelog.txt", "w")
#filelog.close()