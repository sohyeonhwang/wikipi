#!/usr/bin/env python3

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
regex_lists_dir = wikipi_repo_path / 'regex' / 'allregex_plus'

# local raw_data directory ath
dumps_dir = wikipi_repo_path.parent / 'raw_data' / 'wmf_20190901'
editions = ['fr','en','es']

## GENERATE REGEX WIDE TASK LIST
print("------------------------------------------------------------------------")
print("Now making the wide tasklist.")
print("------------------------------------------------------------------------")

tasklist_wide_all = open("./output/tasklist_wide_all", 'w',encoding='utf-8')
#tasklist_wide = open("./output/tasklist_wide",'w',encoding='utf-8')
for edition in editions:
	output_filename = "./output/tasklist_wide_plus_{}".format(edition)
	tasklist_wide = open(output_filename, 'w',encoding='utf-8')

	regexes = []
	regex_label_pairs = {}

	# find the file with the list of regexes
	regex_list_file = "{}_all_plus_list.tsv".format(edition)
	regex_file_path = regex_lists_dir / regex_list_file

	with open(regex_file_path.as_posix()) as allregex_tsv:
		reader =csv.reader(allregex_tsv, delimiter='\t')
		i = 1
		for row in reader:
			label = "{}_{}".format(i,str(row[0]).upper().replace(" ", "_"))
			i += 1
			#print(label)
			regex = row[2]
			#print(regex)
			#print(regex[:-1] + '\\b)')
			regex_label_pairs[label] = regex
			#print("  ")

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
			taskline = "python3 ./mwdumptools/wikiq ./input/{} -o ./output {}\n".format(input,regex)
			tasklist_wide.write(taskline)
			tasklist_wide_all.write(taskline)
			count += 1
	print("{} task lines written.".format(count))
	tasklist_wide.close()
tasklist_wide_all.close()
