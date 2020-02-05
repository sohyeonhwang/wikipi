import sys
# add pyspark to your python path e.g.
#sys.path.append("/home/nathante/sparkstuff/spark/python/pyspark")
#sys.path.append("/home/nathante/sparkstuff/spark/python/")
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql import types
import argparse
import glob
from os import mkdir
from os import path
from wikiq_util import PERSISTENCE_RADIUS
#read a table

def parse_args():

    parser = argparse.ArgumentParser(description='Create a dataset of edits by user.')
    parser.add_argument('-i', '--input-file', help='Tsv file of wiki edits. Supports wildcards ', required=True, type=str)
    parser.add_argument('-o', '--output-dir', help='Output directory', default='./output', type=str)
#    parser.add_argument('--wiki', help="Wiki name. If not provided, we will guess based on the filename.", type=str)
    parser.add_argument('--urlencode', help="whether we need to decode urls",action="store_true")
    parser.add_argument('--output-format', help = "[csv, parquet] format to output",type=str)
    parser.add_argument('--num-partitions', help = "number of partitions to output",type=int, default=1)
    parser.add_argument('--schema-opt', help = 'Options for the input schema.', choices = ["basic","persistence","collapse","persistence+collapse"])
#    parser.add_argument('--nodes', help = "how many hyak nodes to use", default=0, type=int)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    args = parse_args()
    conf = SparkConf().setAppName("Wiki Users Spark")
    spark = SparkSession.builder.getOrCreate()


    files = glob.glob(args.input_file)
    files = [path.abspath(p) for p in files]


    