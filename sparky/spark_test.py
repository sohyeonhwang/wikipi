import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql import types
import argparse
import glob
import csv
import re
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

    reader = spark.read

    wiki_2_df = reader.csv(files,
                    sep="\t",
                    inferSchema=False,
                    header=True,
                    mode="PERMISSIVE")
    
    wiki_2_df = wiki_2_df.repartition(args.num_partitions)
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

    # metadata columns
    metaColumns = struct.fieldNames()
    meta_df = wiki_2_df.select(*metaColumns)
    #meta_df.orderBy("articleid").show()

    # regex columns
    regexDFColumns = [c for c in wiki_2_df.columns if c[0].isdigit()]
    regexDFColumns.append("revid")
    regexDFColumns.append("date_time")
    regexDFColumns.append("articleid")
    regex_df = wiki_2_df.na.replace('None',None).select(*regexDFColumns)
    #regex_df.show(vertical=True)
    #print(regexDFColumns)

    # combine the regex columns into one column, if not None/null
    # this has: revid, article_id, date/time, regexes
    onlyRegexCols = [c for c in regex_df.columns if c[0].isdigit()]
    regexes_revid_df = regex_df.select(regex_df.revid,regex_df.articleid, regex_df.date_time,f.concat_ws(', ',*onlyRegexCols).alias("REGEXES"))
    #regexes_revid_df.show(vertical=True, truncate=False)

    regex_diff_df = regex_df.orderBy("articleid")
    #regex_diff_df.show()

    #for c in onlyRegexCols:
    #    print(c)
    #    print(type(c))

    # finding the 'WP' and 'Wikipedia' regex errors -- basically a result that does not have ':'
    def ff(revision):
        for c in onlyRegexCols:
            # if there is a regex that's been found for a policy...
            if (revision[c] is not None):
                print(revision[c])
                # the conditions wherein we know that there is a WP or Wikipedia somewhere
                if (":" not in revision[c]):
                    print("PROBLEM: {}".format(revision[c]))
                elif (re.search(r"(WP|Wikipedia)$",revision[c]) is not None):
                    print("PROBLEM: {}".format(revision[c]))
                elif ("WP," in revision[c]) or ("Wikipedia," in revision[c]):
                    print("PROBLEM: {}".format(revision[c]))
            else:
                continue
    
    regex_df.foreach(ff)
    # for each row, if there is no ':' in the column result 
