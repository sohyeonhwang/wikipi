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
import numpy as np
import traceback

start_time = time.time()

def parse_args():
    parser = argparse.ArgumentParser(description='Create a dataset.')
    parser.add_argument('-i', '--input', help='Path for directory of wikiq tsv outputs', default='/gscratch/comdata/raw_data/sohw_wikiq_outputs_202302/R12_R2_R3', type=str)
    parser.add_argument('--lang', help='Specify which language edition', default='es',type=str)
    parser.add_argument('-o', '--output-directory', help='Output directory', default='./../output_spark_202302', type=str)
    parser.add_argument('--num-partitions', help = "number of partitions to output",type=int, default=1)
    parser.add_argument('-c','--chunks', help = "number of partitions to output",type=int, default=1)
    parser.add_argument('-r','--rule', help='Specify the rule column name', default="R12_R2_R3",type=str)
    args = parser.parse_args()
    return(args)

if __name__ == "__main__":
    args = parse_args()

    print("STARTING CODE TO PYSPARK THE WIKIQ OUTPUTS.")

    # checking args and retrieving inputs
    print("\t  LANG:\t{}".format(args.lang))

    if not os.path.isdir(args.output_directory):
        os.mkdir(args.output_directory)

    # input files in the directory variable
    directory = "{}/{}wiki*".format(args.input, args.lang)
    files = glob.glob(directory)#[:10]
    print("# tsvs, {}wiki: {}\n".format(args.lang, len(files)))
    # chunk the files (default is 1)
    files_chunked = np.array_split(files, args.chunks)
    print("> Split the file list into {} chunks.".format(len(files_chunked)))

    #specify rule as string
    rule_columns = args.rule.split("_")
    print("> Handling {} rules".format(len(rule_columns)))

    # start the spark session and context
    conf = SparkConf().setAppName("Wiki Regex Spark")
    spark = SparkSession.builder.getOrCreate()
    reader = spark.read
    print("> Started the Spark session...")

    # setting up spark reader and build a schema
    reader = spark.read
    for rule_column in rule_columns:
        struct = types.StructType().add(rule_column,types.StringType(),True)
    struct = struct.add("anon",types.BooleanType(),True)
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

    n_out = 1
    try:
        for chunk in files_chunked:
            chunk = chunk.tolist()
            print("> Reading the files...")
            df = reader.csv(chunk,
                            sep='\t',
                            inferSchema=False,
                            header=True,
                            mode="PERMISSIVE",
                            schema = struct)

            #df = df.withColumnRenamed(rule_column,"detected_regexes")
            # if we want to check the columns and types
            #for col in df.dtypes:
            #    print(col[0]+" , "+col[1])

            df = df.withColumn('month',f.month(df.date_time))
            df = df.withColumn('year',f.year(df.date_time))
            df = df.na.replace({'None': None},subset=rule_columns)
            df = df.na.replace({'': None},subset=rule_columns)

            print("> We're dealing with {} rows of data...".format(df.count()))
            print("> Using {} partitions...".format(df.rdd.getNumPartitions()))

            _temp = df.na.drop(how="any",subset=rule_columns) 
            print(_temp.show(n=3, vertical=True))
            print("> Filtered for only invocation revisions is {} rows of data...".format(_temp.count()))

            _temp.coalesce(1).write.csv("{}/{}wiki_filtered_for_rule_invocations_{}.tsv".format(args.output_directory,args.lang,n_out),sep='\t',mode='append',header=True)
            n_out += 1
        spark.stop()
    except Exception:
        traceback.print_exc()
        spark.stop()

"""
    print("> Partitioning windows by article to get ...".format(df.rdd.getNumPartitions()))
    # set up to track instances of NEW invocations per invocation
    my_window = Window.partitionBy("articleid").orderBy("date_time")
    df = df.withColumn("regex_prev", f.lag(df.detected_regexes).over(my_window))
    df.show(n=5)

    print("> Using {} partitions...".format(df.rdd.getNumPartitions()))

    input("?")
    # to track how many revisions have a new regex (0 for no, 1 for yes)
    df = df.withColumn("regex_diff_bool", f.when(df.REGEX_WIDE == df.regex_prev, 0).otherwise(1))
    # to keep track of actual additions
    df = df.withColumn("regex_diff_string", lit("THERE_WAS_NO_REGEX_DIFFERENCE_SINCE_PREVIOUS_REVISION").cast(types.StringType()))
    df = df.withColumn("regex_diff_count", lit(0).cast(types.LongType()))

    # only revisions with a different set of invocations from previous
    df_filtered = df.where("regex_diff_bool == 1")
    print(df_filtered.count())

    # TESTING that df_filtered is a reasonable df to work with
    df.groupBy("year","month").agg(f.sum("regex_diff_bool").alias("num_revs_with_regex_diff")).orderBy(df.year, df.month).show(n=10)
    df_filtered.groupBy("year","month").agg(f.sum("regex_diff_bool").alias("num_revs_with_regex_diff")).orderBy(df_filtered.year, df_filtered.month).show(n=10)

    #_temp = df.filter(df.REGEX_WIDE.isNotNull())
    #print(_temp.show(n=3, vertical=True))
    #print(_temp.count())

    spark.stop()
"""