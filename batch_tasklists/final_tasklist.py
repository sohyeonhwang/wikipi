#!/usr/bin/env python3

import re
import csv
from pathlib import Path
import os

wikipi_repo_path = Path(os.getcwd()).parent
out_path_base = Path(os.getcwd()) / 'output'

outputs = os.listdir(out_path_base.as_posix())

print(outputs)
