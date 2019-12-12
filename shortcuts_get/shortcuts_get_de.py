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
# german
de_pol_gl_list_pg = requests.get("https://de.wikipedia.org/wiki/Wikipedia:Richtlinien").text




# create a log for what's being done?
#filelog = open("filelog.txt", "w")
#filelog.close()
