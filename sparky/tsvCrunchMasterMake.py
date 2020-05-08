import sys
import pandas as pd
import argparse
import glob, os, re
import csv
from pathlib import Path
from datetime import datetime
import time

start_time = time.time()

def parse_args():
    parser = argparse.ArgumentParser(description='Create a dataset.')
    parser.add_argument('-i', '--input', help='Path for directory of wikiq tsv outputs', required=True, type=str)
    parser.add_argument('--lang', help='Specify which language edition', default='es',type=str)
    parser.add_argument('-o', '--output-dir', help='Output directory', default='./tsvCrunchOutput', type=str)
    parser.add_argument('-ofn', '--output-filename', help='filename for the output file of tsv', default='testCrunch', type=str)
    parser.add_argument('--num-partitions', help = "number of partitions to output",type=int, default=1)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    args = parse_args()

    # checking args and retrieving inputs
    print("INPUT:\t{}".format(args.input))
    print("LANG ED:\t{}".format(args.lang))
    print("O_DIR:\t{}".format(args.output_directory))
    print("O_FN:\t{}".format(args.output_filename))

    if not os.path.isdir(args.output_dir):
        os.mkdir(args.output_dir)

    # e.g. /gscratch/comdata/users/sohw/wikipi/wikiq_runs/output_samples/tsvSampleInputs
    directory = "{}/{}wiki/*.tsv".format(args.input,args.lang)
    print("INPUT PATH:{}".format(directory))

    files = glob.glob(directory)
    files = [os.path.abspath(p) for p in files]
    print("Number of tsvs to process: {}\n".format(len(files)))

    # combine tsvs into ONE tsv first
    print("Building master tsv file ...")
    i = 1

    out_filepath = "{}/{}{}.tsv".format(args.output_dir,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))

    # alternative method with pandas
    combined_tsv = pd.concat([pd.read_csv(tsv_f, sep='\t') for tsv_f in files])
    combined_tsv.to_csv(out_filepath, sep='\t')
    print("Verify: {}".format(out_filepath))
    # this should give a horribly long tsv file that we can input into a spark session.

"""
    # methods with loop
    master_tsv = open(out_filepath, 'w')

    for tsv_f in files:
        print("Looking at {} of {}: {}\n".format(i, len(files),tsv_f))
        # for the first one, we just copy the whole thing
        if i == 1:
            with open(tsv_f, "r") as tsv:
                reader = csv.reader(tsv,delimiter="\t")
                for line in reader:
                    master_tsv.write(line)

        # for the rest, we don't include header.
        else:
            with open(tsv_f, "r") as tsv:
                reader = csv.reader(tsv,delimiter="\t")
                line_count = 0
                for line in reader:
                    line_count += 1
                    if line_count == 1:
                        continue
                    master_tsv.write(line)

        i += 1

    master_tsv.close()
    print("Verify: {}".format(out_filepath))
    # this should give a horribly long tsv file that we can input into a spark session.
"""
