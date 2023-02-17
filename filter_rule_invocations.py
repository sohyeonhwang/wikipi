#!/usr/bin/env python3
"""
This inputs wikiq outputs to filter down only to revisions that have detected rule invocations.
"""

import sys, os
import pandas as pd
import numpy as np
import argparse
from pathlib import Path
from datetime import datetime
from multiprocessing import Pool, cpu_count

def parse_args():
    parser = argparse.ArgumentParser(description='Create a dataset.')
    #parser.add_argument('-i', '--input', help='wikiq output file to input', required=True, type=str)
    parser.add_argument('--lang', help='Specify which language edition', default='en',type=str)
    parser.add_argument('-o', '--output-directory', help='Output directory', default='./../output_filter_rule_invocations', type=str)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    args = parse_args()
    cores = cpu_count()

    # checking args and retrieving inputs
    #print("\t   INPUT:\t{}".format(args.input))
    print("\t    LANG:\t{}".format(args.lang))
    print("\t OUT_DIR:\t{}".format(args.output_directory))

    if not os.path.isdir(args.output_directory):
        print("> Output directory does not exist, making now.")
        os.mkdir(args.output_directory)

    f = Path(os.getcwd()).parent / "output" / args.input
    print(f)
    input("?")

    # read input file
    dtypes = {'"REGEX_WIDE"':np.str, 'anon':np.bool, 'articleid':np.int64,'date_time':np.str,'deleted':np.bool,'editor':np.str,'editorid':np.int64,'minor':np.bool,'namespace':np.int16,'revert':np.bool,'reverteds':np.str,'revid':np.int64,'sha1':np.str,'text_chars':np.int16,'title':np.str}
    parse_dates_in = ['date_time']
    dfs = pd.read_csv(f ,sep="\t", header=0, dtype=dtypes, parse_dates=parse_dates_in, chunksize=100000)
    dfs.rename(columns={'"REGEX_WIDE"':'detected_regexes'},inplace=True)

