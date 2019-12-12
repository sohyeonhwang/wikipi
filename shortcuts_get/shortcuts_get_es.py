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
es_pol_list_pg = requests.get("https://es.wikipedia.org/wiki/Categor%C3%ADa:Wikipedia:Pol%C3%ADticas").text
es_gl_list_pg = requests.get("https://es.wikipedia.org/wiki/Categor%C3%ADa:Wikipedia:Convenciones").text

# https://es.wikipedia.org/wiki/Wikipedia:Lista_de_pol%C3%ADticas_y_convenciones
#https://es.wikipedia.org/wiki/Categor%C3%ADa:Wikipedia:Pol%C3%ADticas
#https://es.wikipedia.org/wiki/Categor%C3%ADa:Wikipedia:Convenciones


list_pages = [es_pol_list_pg,es_gl_list_pg]
list_pages_names = ["es_policies","es_guidelines"]

#for each page listing policy or guideline
for i in range(0,len(list_pages)):
    p = list_pages[i]
    
    #create a new tsv file for output later
    filename = '{}.tsv'.format(list_pages_names[i])
    print(filename)

    #create the html soup to work with
    soup = BeautifulSoup(p, 'html.parser')
    #print(soup.title.string)


    #narrow down on the parts with the relevant links
    section = soup.select("#mw-pages > div.mw-content-ltr > div.mw-category")
    outlinks = section[0].find_all("a")

    #sanity check
    print(len(outlinks))
    print(outlinks)

    # initialize the rows of info
    rows = []

    # for every link to a policy thing
    for l in outlinks:
        #get some info
        cat = "NA"
        url = 'https://es.wikipedia.org{}'.format(l.get('href'))

        title = l.string

        if "Plantilla:Políticas" == title:
            continue

        print(title)

        #go to the policy page
        outlink_pg = requests.get(url).text
        minisoup = BeautifulSoup(outlink_pg, 'html.parser')

        #temporary list of the shortcuts
        temp = []

        # find the shortcuts
        # add title as it is also a hyperlink thing, which should be the same as pagetitle actually...
        temp.append(minisoup.find("h1").string)
        #print(minisoup.find("h1").string)

        #go through the boxes with shortcuts
        links_box = minisoup.select("#shortcut > a.external.text")
        #print(links_box)
        
        
        for sc in links_box:
            temp.append(sc.string)
            print(sc.string)
        
        #remove any duplicates
        temp = list(dict.fromkeys(temp))

        #update the data structure
        row = [cat,url,title,temp]
        rows.append(row)

    style_manual_links = ["/wiki/Wikipedia:Convenciones_idiomáticas",
    "/wiki/Wikipedia:Manual_de_estilo","/wiki/Wikipedia:Manual_de_estilo/Abreviaturas",
    "/wiki/Wikipedia:Manual_de_estilo/Biografías","/wiki/Wikipedia:Manual_de_estilo/Páginas_de_desambiguación",
    "/wiki/Wikipedia:Manual_de_estilo/Artículos_relacionados_con_Japón","/wiki/Wikipedia:Manual_de_estilo/Marcas",
    "/wiki/Wikipedia:Manual_de_estilo/Páginas_de_desambiguación","/wiki/Wikipedia:Manual_de_estilo/Secciones_de_curiosidades"]

    if "guidelines" in filename:
        for l in style_manual_links:
            cat = "NA"
            url = 'https://es.wikipedia.org{}'.format(l)

            #go to the policy page
            outlink_pg = requests.get(url).text
            minisoup = BeautifulSoup(outlink_pg, 'html.parser')
            title = minisoup.find("h1").string
            print(title)

            # find the shortcuts
            temp = []
            temp.append(title)
            links_box = minisoup.select("#shortcut > a.external.text")
            #print(links_box)

            for sc in links_box:
                temp.append(sc.string)
                print(sc.string)
            
            #remove any duplicates
            temp = list(dict.fromkeys(temp))

            #update the data structure
            row = [cat,url,title,temp]
            rows.append(row)


    #generate the entries
    print("creating the tsv file now....")
    with open(filename, 'w') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')
        writer.writerows(rows)
    print(" ")



# create a log for what's being done?
#filelog = open("filelog.txt", "w")
#filelog.close()