import sys
import glob
from pathlib import Path
import os
import csv
import re
from bs4 import BeautifulSoup
import requests


cwd = Path(os.getcwd())

de_path = cwd.parent / "de_rule_links.txt"
es_path = cwd / "shortcuts_get" / "output" / "es_all_plus.tsv"
fr_path = cwd / "shortcuts_get" / "output" / "fr_all_plus.tsv"
en_path = cwd / "shortcuts_get" / "output" / "en_all_plus.tsv"
ja_path = cwd / "shortcuts_get" / "output" / "ja_policies-guidelines.tsv"
paths = {'es':es_path.as_posix(),'fr':fr_path.as_posix(),'ja':ja_path.as_posix(),'en':en_path.as_posix(),'de':de_path.as_posix()}

# language edition
#   > total
#   > # of ILLs to the other 4
#   > which lang eds
#   > # of ILLs to other 4, being rule pages
#   > which lang eds

#GLOBAL COUNTS
total = 0

linked_counts = {0:0,1:0,2:0,3:0,4:0}
linkedrules_counts = {0:0,1:0,2:0,3:0,4:0}

all_ills_counts = []

#german
de_file = open(de_path.as_posix(),"r")
de_total = 0

# for each rule page

de_extracted = {}
for line in de_file:
    de_total += 1
    total += 1

    url = line
    print(url)

    de_extracted[url]={'totalILLs':0,'otherFour':[],'otherFourCount':0,'otherFour_Rules':[],'otherFourCount_Rules':0}

    outlink_pg = requests.get(url).text
    soup = BeautifulSoup(outlink_pg, "html.parser")

    # find the ILLs
    lang_portal = soup.select("#p-lang > div.body > ul > li > a")

    # count # of ILLs
    de_extracted[url]['totalILLs'] = len(lang_portal)
    all_ills_counts.append(de_extracted[url]['totalILLs'])
    linked = []
    linked_rule = []

    # for each ILL
    for l in lang_portal:
        attributes = l.attrs 
        if attributes['lang'] in ['ja','en','es','fr']:
            linked.append(attributes['lang'])
            print(attributes['lang'])
            #now go to that page and check if it's a rule page
            new_url = attributes['href']
            print('!!!!!! ASSOCIATED RULE PAGE? {}'.format(new_url))
            filetocheck = paths[attributes['lang']]
            with open(filetocheck) as fd:
                rd = csv.reader(fd, delimiter="\t")
                for row in rd:
                    if new_url in row:
                        linked_rule.append(attributes['lang'])
                        break
    
    de_extracted[url]['otherFour'] = linked
    de_extracted[url]['otherFourCount'] = len(linked)
    linked_counts[len(linked)] += 1
    de_extracted[url]['otherFour_Rules'] = linked_rule
    de_extracted[url]['otherFourCount_Rules'] = len(linked_rule)
    linkedrules_counts[len(linked_rule)] += 1

    print(de_extracted[url])
    print(linked_counts)

de_file.close()

#others
totals = {'de':de_total,'en':0,'fr':0,'es':0,'ja':0}

### at this point we have:
#total 
#totals
#de_extracted
#linkedrules_counts
#linked_counts

# for each lang edition
#paths = {'es':es_path.as_posix(),'fr':fr_path.as_posix(),'ja':ja_path.as_posix(),'en':en_path.as_posix(),'de':de_path.as_posix()}

es_extracted = {}
fr_extracted = {}
en_extracted = {}
ja_extracted = {}
#de_extracted[url]={'totalILLs':0,'otherFour':[],'otherFourCount':0,'otherFour_Rules':[],'otherFourCount_Rules':0}

es_total = 0
fr_total = 0
en_total = 0
ja_total = 0

variables_extracted = {'es':es_extracted,'fr':fr_extracted,'en':en_extracted,'ja':ja_extracted}
variables_totals = {'es':es_total,'fr':fr_total,'en':en_total,'ja':ja_total,'de':de_total}

for p in ['en','fr','es','ja']:
    with open(paths[p], "r") as tsv:
        reader = csv.reader(tsv,delimiter="\t")

        # we look at each rule
        for row in reader:
            total += 1
            variables_totals[p] += 1

            temp_extracted = variables_extracted[p]

            url = row[1]
            print('{}\n'.format(url))
            temp_extracted[url] = {'totalILLs':0,'otherFour':[],'otherFourCount':0,'otherFour_Rules':[],'otherFourCount_Rules':0}

            outlink_pg = requests.get(url).text
            soup = BeautifulSoup(outlink_pg, "html.parser")

            # find the ILLs
            lang_portal = soup.select("#p-lang > div.body > ul > li > a")

            # count # of ILLs
            temp_extracted[url]['totalILLs'] = len(lang_portal)
            all_ills_counts.append(temp_extracted[url]['totalILLs'])
            linked = []
            linked_rule = []

            # for each ILL
            for l in lang_portal:
                attributes = l.attrs
                if attributes['lang'] in ['ja','en','es','fr','de']:
                    linked.append(attributes['lang'])
                    print(attributes['lang'])
                    #now go to that page and check if it's a rule page
                    new_url = attributes['href']
                    print('!!!!!! ASSOCIATED RULE PAGE? {}'.format(new_url))
                    filetocheck = paths[attributes['lang']]
                    with open(filetocheck) as fd:
                        rd = csv.reader(fd, delimiter="\t")
                        for row in rd:
                            if new_url in row:
                                linked_rule.append(attributes['lang'])
                                break

            #update the variables
            temp_extracted[url]['otherFour'] = linked
            temp_extracted[url]['otherFourCount'] = len(linked)
            linked_counts[len(linked)] += 1
            temp_extracted[url]['otherFour_Rules'] = linked_rule
            temp_extracted[url]['otherFourCount_Rules'] = len(linked_rule)
            linkedrules_counts[len(linked_rule)] += 1

            print(temp_extracted[url])
            print(linked_counts)
            print(linkedrules_counts)


### at this point we have:
# total 
# totals
# de_extracted, es_extracted, en_extracted, ja_extracted, fr_extracted
# linkedrules_counts
# linked_counts

print('\n======================================================================\n')
input("Press Enter to continue...")

print('TOTAL # RULES: {}'.format(total))
print('TOTALS # RULES BY LANG EDITION {}'.format(variables_totals))
print('# of RULES WITH # ILLs (within top 5): {}'.format(linked_counts))
print('# of RULES WITH # ILLs TO OTHER RULES (within top 5): {}'.format(linkedrules_counts))

#{'totalILLs':0,'otherFour':[],'otherFourCount':0,'otherFour_Rules':[],'otherFourCount_Rules':0}

#       de  en  es  fr  ja
# de    -   
# en
# es
# fr
# ja 

extracts = {'de':de_extracted, 'es':es_extracted, 'en':en_extracted, 'ja':ja_extracted, 'fr':fr_extracted}

de_counts = [0,0,0,0,0]
en_counts = [0,0,0,0,0]
es_counts = [0,0,0,0,0]
fr_counts = [0,0,0,0,0]
ja_counts = [0,0,0,0,0]

counts = {'de':de_counts, 'es':es_counts, 'en':en_counts, 'ja':ja_counts, 'fr':fr_counts}

de_counts_rules = [0,0,0,0,0]
en_counts_rules = [0,0,0,0,0]
es_counts_rules = [0,0,0,0,0]
fr_counts_rules = [0,0,0,0,0]
ja_counts_rules = [0,0,0,0,0]

counts_rules = {'de':de_counts_rules, 'es':es_counts_rules, 'en':en_counts_rules, 'ja':ja_counts_rules, 'fr':fr_counts_rules}

order_of_counts_lists = {'de':0,'en':1,'es':2,'fr':3,'ja':4}

for i in ['de','en','es','fr','ja']:
    extracted_temp = extracts[i]
    for e in extracted_temp:
        linked = extracted_temp[e]['otherFour']
        linked_rules = extracted_temp[e]['otherFour_Rules']
        for l in linked:
            i_num = order_of_counts_lists[l]
            counts[i][i_num] += 1
        for l in linked_rules:
            i_num = order_of_counts_lists[l]
            counts_rules[i][i_num] += 1

print("\n\n COUNTS\n")
print('\tde\ten\tes\tfr\tja')
print('de\t{}\t{}\t{}\t{}\t{}'.format(de_counts[0],de_counts[1],de_counts[2],de_counts[3],de_counts[4]))
print('en\t{}\t{}\t{}\t{}\t{}'.format(en_counts[0],en_counts[1],en_counts[2],en_counts[3],en_counts[4]))
print('es\t{}\t{}\t{}\t{}\t{}'.format(es_counts[0],es_counts[1],es_counts[2],es_counts[3],es_counts[4]))
print('fr\t{}\t{}\t{}\t{}\t{}'.format(fr_counts[0],fr_counts[1],fr_counts[2],fr_counts[3],fr_counts[4]))
print('ja\t{}\t{}\t{}\t{}\t{}'.format(ja_counts[0],ja_counts[1],ja_counts[2],ja_counts[3],ja_counts[4]))


print("\n COUNTS RULES\n")
print('\tde\ten\tes\tfr\tja')
print('de\t{}\t{}\t{}\t{}\t{}'.format(de_counts_rules[0],de_counts_rules[1],de_counts_rules[2],de_counts_rules[3],de_counts_rules[4]))
print('en\t{}\t{}\t{}\t{}\t{}'.format(en_counts_rules[0],en_counts_rules[1],en_counts_rules[2],en_counts_rules[3],en_counts_rules[4]))
print('es\t{}\t{}\t{}\t{}\t{}'.format(es_counts_rules[0],es_counts_rules[1],es_counts_rules[2],es_counts_rules[3],es_counts_rules[4]))
print('fr\t{}\t{}\t{}\t{}\t{}'.format(fr_counts_rules[0],fr_counts_rules[1],fr_counts_rules[2],fr_counts_rules[3],fr_counts_rules[4]))
print('ja\t{}\t{}\t{}\t{}\t{}'.format(ja_counts_rules[0],ja_counts_rules[1],ja_counts_rules[2],ja_counts_rules[3],ja_counts_rules[4]))


print('\n================================DONE')


'''
TOTAL # RULES: 743
TOTALS # RULES BY LANG EDITION {'es': 100, 'ja': 112, 'fr': 127, 'en': 331, 'de': 73}
# of RULES WITH # ILLs (within top 5): {0: 245, 1: 95, 2: 86, 3: 106, 4: 211}
# of RULES WITH # ILLs TO OTHER RULES (within top 5): {0: 381, 1: 107, 2: 90, 3: 99, 4: 66}


 COUNTS

        de      en      es      fr      ja
de      0       47      33      31      33
en      105     0       103     113     117
es      58      78      0       65      59
fr      53      92      61      0       71
ja      60      91      67      68      0

 COUNTS RULES

        de      en      es      fr      ja
de      0       33      19      21      22
en      34      0       57      64      69
es      20      57      0       39      46
fr      21      64      38      0       45
ja      22      68      45      45      0
'''