#!/usr/bin/env python3

import re
import csv
from pathlib import Path
import os

wikipi_repo_path = Path(os.getcwd()).parent
output_path_base = Path(os.getcwd()) / 'output'

# input files
input_file = "tasklist_wide_en"
input_path = output_path_base / input_file

# output files
output1 = open("./output/{}_split1".format(input_file),"a", encoding="utf-8")
output2 = open("./output/{}_split2".format(input_file),"a", encoding="utf-8")
output3 = open("./output/{}_split3".format(input_file),"a", encoding="utf-8")
output4 = open("./output/{}_split4".format(input_file),"a", encoding="utf-8")

with open(input_path.as_posix()) as tasklist:
    reader = csv.reader(tasklist)
    r = 0
    for row in reader:
        r += 1

        line = "{}{}{}{}{}{}{}{}{}{}{}{}{}\n".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12])

        if r <= 165:
            output1.write(line)
        elif 165 < r <= 330:
            output2.write(line)
        elif 330 < r <= 495:
            output3.write(line)
        elif 496 < r:
            output4.write(line)

output1.close()
output2.close()
output3.close()
output4.close()
