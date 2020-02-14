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

    reader = spark.read

    wiki2df = reader.csv(files,
                    sep="\t",
                    inferSchema=False,
                    header=True,
                    mode="PERMISSIVE",
                    schema=struct)
    
    wiki2df = wiki2df.repartition(args.num_partitions)

    wiki2df.show()