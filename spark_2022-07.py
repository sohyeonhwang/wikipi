import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import lit
from pyspark.sql import types
import argparse
import glob, os
import csv
from datetime import datetime
import time
import collections 

start_time = time.time()

def parse_args():
    parser = argparse.ArgumentParser(description='Create a dataset.')
    parser.add_argument('-i', '--input', help='Path for directory of wikiq tsv outputs', required=True, type=str)
    parser.add_argument('--lang', help='Specify which language edition', default='es',type=str)
    parser.add_argument('-o', '--output-directory', help='Output directory', default='./spark_2022-07_output', type=str)
    parser.add_argument('-ofn', '--output-filename', help='filename for the output file of tsv', default='test', type=str)
    parser.add_argument('--num-partitions', help = "number of partitions to output",type=int, default=1)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    args = parse_args()

    # checking args and retrieving inputs
    print("INPUT:\t{}".format(args.input))
    print(" LANG:\t{}".format(args.lang))
    print("O_DIR:\t{}".format(args.output_directory))
    print(" O_FN:\t{}".format(args.output_filename))

    if not os.path.isdir(args.output_directory):
        os.mkdir(args.output_directory)

    # start the spark session and context
    #conf = SparkConf().setAppName("wiki regex spark processing")
    #spark = SparkSession.builder.getOrCreate()
    #reader = spark.read
    #print("Started the Spark session...\n")

    # input files in the directory
    directory = "{}/{}wiki*".format(args.input, args.lang)
    files = glob.glob(directory)
    print("# files for {}wiki: {}".format(args.lang, len(files)))
