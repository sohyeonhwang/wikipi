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

def parse_args():
    parser = argparse.ArgumentParser(description='Create a dataset.')
    parser.add_argument('-i', '--input-file', help='Tsv file of wiki edits. Supports wildcards ', required=True, type=str)
    parser.add_argument('-o', '--output-dir', help='Output directory', default='./output', type=str)
    parser.add_argument('--num-partitions', help = "number of partitions to output",type=int, default=1)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    args = parse_args()
    conf = SparkConf().setAppName("Wiki Regex Spark")
    spark = SparkSession.builder.getOrCreate()

    files = glob.glob(args.input_file)
    files = [os.path.abspath(p) for p in files]

    print("We are now sparking {}".format(files[0]))

    reader = spark.read

    wiki_2_df = reader.csv(files,
                    sep="\t",
                    inferSchema=False,
                    header=True,
                    mode="PERMISSIVE")
    
    wiki_2_df = wiki_2_df.repartition(args.num_partitions)
    wiki_2_df.replace('None',None)

    #wiki_2_df.show()
    #wiki_2_df.describe().show()

    struct = types.StructType().add("anon",types.StringType(),True)
    struct = struct.add("articleid",types.LongType(),True)
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


    # meteadata columns
    metaColumns = struct.fieldNames()
    meta_df = wiki_2_df.select(*metaColumns)
    #meta_df.show()

    # regex columns
    regexDFColumns = [c for c in wiki_2_df.columns if c[0].isdigit()]
    regexDFColumns.append("revid")
    regex_df = wiki_2_df.select(*regexDFColumns)
    #regex_df.show(vertical=True)
    #print(regexDFColumns)

    print("COMBINING regex COLUMNS")
    def columnCombine(row):
        onlyRegexCols = [c for c in wiki_2_df.columns if c[0].isdigit()]
        for c in onlyRegexCols:
            if (onlyRegexCols[c] != None) or (onlyRegexCols[c]!='null'):
                print(row[c])

    def test(row):
        row.revid

    regex_df.foreach(test)
