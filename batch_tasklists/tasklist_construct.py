#!/usr/bin/env python3

#script for creating the ILL matrix

import re
import csv
from pathlib import Path
import os

wikipi_repo_path = Path(os.getcwd()).parent
out_path_base = Path(os.getcwd()) / 'output'

# basic construction:
# "python3 ./mwdumptools/wikiq {} -o ./output -RP '{}' -RPl {}".format(input_file_name, regex, label)"

# regex directory paths
giant_regex_dir = wikipi_repo_path / 'regex' / 'giant-regexes'
regex_lists_dir = wikipi_repo_path / 'regex' / 'regex-lists'

# local raw_data directory ath
dumps_dir = wikipi_repo_path.parent / 'raw_data' / 'wmf_20190901'
editions = ['en','fr','ja','es']


## GENERATE BABY TASK LIST FOR TEST BATCHING
print("------------------------------------------------------------------------")
print("Now making the baby tasklist for testing the batch job set-up.")
print("------------------------------------------------------------------------")

babytasklist = open("./output/babytasklist",'w',encoding='utf-8')
for edition in editions:
	print("Generating from {} language edition".format(edition))
	regex_label_pairs = {'EN_ALL_NPOV':'(?:\bW(?:P:(?:(?:S(?:UB(?:STANTIAT|JECTIV)|TRUCTUR)|FALSEBALANC|(?:UN)?DU)E|N(?:P(?:OV(?:(?:VIE|HO)W|FACT)?|V)|EUTRAL)|A(?:(?:CHIEVE N|TTRIBUTE)POV|ESTHETIC)|P(?:OV(?:NAMING)?|ROPORTION|SCI)|B(?:ALA(?:NCED?|SP)|ESTSOURCES)|W(?:IKIVOICE|EIGHT)|(?:IMPARTI|GEV)AL|(?:YES|RN)POV|TINFOILHAT|VALID|MNA)|ikipedia:Neutral point of view))'}

	# giant regex
	regex_file = "{}_giant-regex.txt".format(edition)
	regex_file_path = giant_regex_dir / regex_file
	f = open(regex_file_path.as_posix(),'r')
	giant_regex = [f.read()]
	regex_label_pairs['ALLPOLICY'] = giant_regex[0]
	labels = [*regex_label_pairs]

	# retrieve the relevant dump files
	input_path = dumps_dir / "{}wiki".format(edition)
	input_files = os.listdir(input_path.as_posix())

	# write the task lines to the file
	count = 0
	for input in input_files[:2]:
		for label in labels:
			regex = regex_label_pairs[label]
			taskline = "python3 ./mwdumptools/wikiq {} -o ./output -RP '{}' -RPl {}\n".format(input,regex,label)
			babytasklist.write(taskline)
			count += 1
	print("{} task lines written.".format(count))
babytasklist.close()


## GENERATE REGEX GIANT TASK LIST
print("------------------------------------------------------------------------")
print("Now making the giant regex tasklist.")
print("------------------------------------------------------------------------")

# create the output file. 'w' for overwrite, 'a' for just append
tasklist_giant = open("./output/tasklist_giant_regexes",'w',encoding='utf-8')

for edition in editions:
	# retrieve the relevant regexes
	regex_file = "{}_giant-regex.txt".format(edition)
	regex_file_path = giant_regex_dir / regex_file
	f = open(regex_file_path.as_posix(),'r')
	giant_regex = [f.read()]
	label = "ALLPOLICY"
	print("Formatting for {} language edition... attaching <{}...>".format(edition,giant_regex[0][:50]))

	# retrieve the relevant dump files
	input_path = dumps_dir / "{}wiki".format(edition)
	input_files = os.listdir(input_path.as_posix())

	# write the task lines to the file
	print("Writing {} task lines.".format(len(input_files)))
	count = 0
	for input in input_files:
		taskline = "python3 ./mwdumptools/wikiq {} -o ./output -RP '{}' -RPl {}\n".format(input,giant_regex[0],label)
		tasklist_giant.write(taskline)
		count += 1
	print("{} task lines written.".format(count))
tasklist_giant.close()


## GENERATE REGEX WIDE TASK LIST
print("------------------------------------------------------------------------")
print("Now making the wide tasklist.")
print("------------------------------------------------------------------------")

tasklist_wide = open("./output/tasklist_wide",'w',encoding='utf-8')
#globbies = list(regex_lists_dir.glob('*allregex_list.tsv'))
#regex_lists_paths = []
#for g in globbies:
#	regex_lists_paths.append(g)
#	print(g)

#print(regex_lists_paths)

for edition in editions:
	regexes = []
	regex_label_pairs = {}

	# find the file with the list of regexes
	regex_list_file = "{}_allregex_list.tsv".format(edition)
	regex_file_path = regex_lists_dir / regex_list_file

	with open(regex_file_path.as_posix()) as allregex_tsv:
		reader =csv.reader(allregex_tsv, delimiter='\t')
		i = 1
		for row in reader:
			label = "{}_{}".format(i,str(row[0]).upper().replace(" ", "_"))
			i += 1
			#print(label)
			regex = row[2]
			regex_label_pairs[label] = regex

	print("Formatted {} regex-label pairs for {} language edition.".format(len(regex_label_pairs),edition))
	labels = sorted([*regex_label_pairs])

	# build the wide_regex line, which will but attached for every input file
	wide_regex = ""
	for label in labels:
		regex = regex_label_pairs[label]
		wide_regex += "-RP '{}' -RPl {} ".format(regex,label)
	wide_regex = wide_regex[:-1]
	regexes.append(wide_regex)


	# retrieve the relevant dump files
	input_path = dumps_dir / "{}wiki".format(edition)
	input_files = os.listdir(input_path.as_posix())

	# write the task lines to the file
	print("Writing {} task lines.".format(len(input_files)))
	count = 0
	for input in input_files:
		for regex in regexes:
			taskline = "python3 ./mwdumptools/wikiq {} -o ./output {}\n".format(input,regex)
			tasklist_wide.write(taskline)
			count += 1
	print("{} task lines written.".format(count))
tasklist_wide.close()
