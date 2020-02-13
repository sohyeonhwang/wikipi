import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql import types
import argparse
import glob
import csv
from pathlib import Path
import os
from wikiq_util import PERSISTENCE_RADIUS
#read a table

def parse_args():

    parser = argparse.ArgumentParser(description='Create a dataset of edits by user.')
    parser.add_argument('-i', '--input-file', help='Tsv file of wiki edits. Supports wildcards ', required=True, type=str)
    parser.add_argument('-o', '--output-dir', help='Output directory', default='./output', type=str)
#    parser.add_argument('--wiki', help="Wiki name. If not provided, we will guess based on the filename.", type=str)
#    parser.add_argument('--urlencode', help="whether we need to decode urls",action="store_true")
#    parser.add_argument('-f','--output-format', help = "[csv, parquet] format to output",type=str)
    parser.add_argument('--num-partitions', help = "number of partitions to output",type=int, default=1)
    parser.add_argument('--schema-opt', help = 'Options for the input schema.', choices = ["basic","persistence","collapse","persistence+collapse"])
#    parser.add_argument('--nodes', help = "how many hyak nodes to use", default=0, type=int)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    args = parse_args()
    conf = SparkConf().setAppName("Wiki Regex Spark")
    spark = SparkSession.builder.getOrCreate()

    # load up the input files
    files = glob.glob(args.input_file)
    files = [path.abspath(p) for p in files]

    read_collapse = args.schema_opt in ["collapse", "persistence+collapse"]
    read_persistence = args.schema_opt in ["persistence", "persistence+collapse"]

    if read_collapse is True:
        struct = struct.add("collapsed_revs", types.IntegerType(), True)

    # specify the schema for the files that we will read in
    struct = struct.add("date_time",types.TimestampType(), True)
    struct = struct.add("deleted",types.BooleanType(), True)
    struct = struct.add("editor",types.StringType(),True)
    struct = struct.add("editor_id",types.LongType(), True)
    struct = struct.add("minor", types.BooleanType(), True)
    struct = struct.add("namespace", types.LongType(), True)
    struct = struct.add("revert", types.BooleanType(), True)
    struct = struct.add("reverteds", types.StringType(), True)
    struct = struct.add("revid", types.LongType(), True)
    struct = struct.add("sha1", types.StringType(), True)
    struct = struct.add("text_chars", types.LongType(), True)
    struct = struct.add("title",types.StringType(), True)

    if read_persistence is True:
        struct = struct.add("token_revs", types.IntegerType(),True)
        struct = struct.add("tokens_added", types.IntegerType(),True)
        struct = struct.add("tokens_removed", types.IntegerType(),True)
        struct = struct.add("tokens_window", types.IntegerType(),True)

    # in particular, we want to also add structures for the regex columns 
    wd = Path(os.getcwd())
    ###  CHANGE THIS BASED ON THE WIKI BEING PROCESSED
    columns_file_path = wd / 'eswiki_columns'

    columnsToMerge = []
    ##wp_columns_struct = wp_columns_struct.add("revid", types.LongType(), True)

    with open(columns_file_path.as_posix()) as tsv:
        rows = csv.reader(tsv, delimiter='\t')
        for row in rows:
            columnsToMerge.append(row[0])
            struct = struct.add(row[0],types.StringType(), True)
            ##wp_columns_struct = wp_columns_struct.add(row[0],types.StringType(), True)

    # create a reader object that can read files
    reader = spark.read

    # reads the data inputted via -i (files) into a Spark DataFrame and repartitions
    df = reader.csv(files,
                    sep="\t",
                    inferSchema=False,
                    header=True,
                    mode="PERMISSIVE",
                    schema=struct)
    df = df.repartition(args.num_partitions)

    # now we want to get make a column that combines all the regex results
    # first we create a variable that's all the columns to coalesce

    for c in df.columns:
        if c[0].isdigit():
            print(c)
