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
# french
fr_pol_list_pg = requests.get("https://fr.wikipedia.org/wiki/Cat%C3%A9gorie:Wikip%C3%A9dia:R%C3%A8gle").text
fr_gl_list_pg = requests.get("https://fr.wikipedia.org/wiki/Cat%C3%A9gorie:Wikip%C3%A9dia:Recommandation").text

# list of X and Y not as good. use the category lists that are automatically generated 
#https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Liste_des_r%C3%A8gles_et_recommandations

#category lists
#https://fr.wikipedia.org/wiki/Cat%C3%A9gorie:Wikip%C3%A9dia:R%C3%A8gle
#https://fr.wikipedia.org/wiki/Cat%C3%A9gorie:Wikip%C3%A9dia:Recommandation

list_pages = [fr_gl_list_pg,fr_pol_list_pg]
list_pages_names = ["fr_guidelines","fr_policies"]

#for each page list of policy or guideline
for i in range(0,len(list_pages)):
    p = list_pages[i]
    
    #create a new tsv file for output later
    filename = "{}.tsv".format(list_pages_names[i])
    print(filename)

    #create the html soup to work with
    soup = BeautifulSoup(p, "html.parser")
    #print(soup.title.string)

    #narrow down on the parts with the relevant links
    section = soup.select("#mw-pages > div.mw-content-ltr > div.mw-category")
    outlinks = section[0].find_all("a")

    # initialize the rows of info
    rows = []

    # for every link to a policy
    for l in outlinks:

        #get some info
        cat = "NA"
        url = "https://fr.wikipedia.org{}".format(l.get('href'))

        title = l.string
        print(title)
    
        #go to the policy page
        outlink_pg = requests.get(url).text
        minisoup = BeautifulSoup(outlink_pg, 'html.parser')

        #temporary list of the shortcuts
        temp = []

        # find the shortcuts
        # add title as it is also a hyperlink thing, which should be the same as pagetitle actually...
        temp.append(minisoup.find("h1").string)
        print(title, url)

        #go through the boxes with shortcuts
        links_box = minisoup.select("div.boite-raccourci.boite-grise.noprint a.mw-redirect")
        for sc in links_box:
            temp.append(sc.string)
        """
        #to include redirects
        links_box = minisoup.select("div.bandeau-simple.bandeau-niveau-information.plainlinks div.boite-raccourci.boite-grise.noprint a.external.text")
        print(links_box[0].get('href'))

        # go to that page and extract those links
        external = requests.get(links_box[0].get('href')).text
        
        babysoup = BeautifulSoup(external, 'html.parser')
        redir_links = babysoup.select("ul#mw-whatlinkshere-list li a.mw-redirect")

        # collect all the redirects
        for link in redir_links:
            print(link.string)
            if "Wiki" or "wiki" in link.string:
                temp.append(link.string)
        """

        #remove any duplicates
        temp = list(dict.fromkeys(temp))

        for t in temp:
            print(t)

        #update the data structure
        row = [cat,url,title,temp]
        rows.append(row)

    print("NOW AT POLICIES EXTRAS")
    if "policies" in filename:
        #if policy, we want to manually add the five pillars
        pillar_links = ["https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Principes_fondateurs",
                        "https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:R%C3%A8gles_de_savoir-vivre",
                        "https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Interpr%C3%A9tation_cr%C3%A9ative_des_r%C3%A8gles",
                        "https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Droit_d%27auteur",
                        "https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Neutralit%C3%A9_de_point_de_vue",
                        "https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Wikip%C3%A9dia_est_une_encyclop%C3%A9die"]

        for url in pillar_links:
            cat = "NA"
            outlink_pg = requests.get(url).text
            minisoup = BeautifulSoup(outlink_pg, 'html.parser')

            temp = []
            title = minisoup.find("h1").string
            temp.append(title)

            print(title, url)

            #go through the boxes with shortcuts
            links_box = minisoup.select("div.boite-raccourci.boite-grise.noprint a.mw-redirect")
            for sc in links_box:
                temp.append(sc.string)

            #remove any duplicates
            temp = list(dict.fromkeys(temp))

            for t in temp:
                print(t)

            #update the data structure
            row = [cat,url,title,temp]
            rows.append(row)

    #sanity check
    #print(len(outlinks))
    #generate the entries
    print("creating the tsv file now....")
    with open(filename, 'w') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')
        writer.writerows(rows)
    print(" ")