import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql import types
import argparse
import glob
import csv
from datetime import datetime
import re
from pathlib import Path
import os
import time 

def parse_args():
    parser = argparse.ArgumentParser(description='Create a dataset.')
    parser.add_argument('-i', '--input-file', help='Tsv file of wiki edits. Supports wildcards ', required=True, type=str)
    parser.add_argument('-o', '--output-dir', help='Output directory', default='./output', type=str)
    parser.add_argument('--num-partitions', help = "number of partitions to output",type=int, default=1)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    start_time = time.time()
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
    # regexes_revid_df.show(vertical=True)

    regex_diff_df = regex_df.orderBy("articleid")
    #regex_diff_df.show()

    #for c in onlyRegexCols:
    #    print(c)
    #    print(type(c))

    # set up to detect errors
    errors = []
    output_filename = "errorSearchOutput_{}".format(files[0][54:])
    print(files[0][54:])
    output_path = Path(os.getcwd()) / "errorSearchOutput" / output_filename
    print(output_path)
    with open(output_path, 'a') as temp:
        print("\n\n\n====================================================================================",file=temp)
        print("OUTPUTTING THE FOLLOWING ON {}".format(datetime.now()),file=temp)
        print("====================================================================================",file=temp)

    # finding the 'WP' and 'Wikipedia' regex errors
    def ff(revision):
        # print(" ")
        for c in onlyRegexCols:
            # if there is a regex that's been found for a policy...
            if (revision[c] is not None):
                # print(revision[c])

                # the conditions wherein we know that there is a WP or Wikipedia somewhere
                if (":" not in revision[c]):
                    errors.append(c)
                    #print("PROBLEM WITH {}: {}".format(c, revision[c]))
                elif (re.search(r"(WP|Wikipedia|Wikipédia)$",revision[c]) is not None):
                    errors.append(c)
                    #print("PROBLEM WITH {}: {}".format(c, revision[c]))
                elif ("WP," in revision[c]) or ("Wikipedia," in revision[c]) or ("Wikipédia," in revision[c]):
                    errors.append(c)
                    #print("PROBLEM WITH {}: {}".format(c, revision[c]))

                # special cases for policies that don't start with WP/Wikipedia
                if (re.search(r"^\d+_(AIDE|PROJET|UTILISATEUR|USUARIA|USUARIO)",c) is not None):
                    if (re.search(r"(Aide|Projet|Utilisateur|Usuaria|Usuario)$",revision[c]) is not None):
                        errors.append(c)
                    elif ("Aide," in revision[c]) or ("Projet," in revision[c]) or ("Utilisateur," in revision[c]) or ("Usuaria," in revision[c]) or ("Usuario," in revision[c]):
                        errors.append(c)

            else:
                continue
        f = open(output_path, 'a')
        print("\n", file=f)
        setOfErrors = set(errors)
        print(setOfErrors, file=f)
    
    regex_df.foreach(ff)
    
    print("RUNNING TIME: {}".format(time.time() - start_time))
