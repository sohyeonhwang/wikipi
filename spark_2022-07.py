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
    parser.add_argument('-i', '--input', help='Path for directory of wikiq tsv outputs', default='./../output', type=str)
    parser.add_argument('--lang', help='Specify which language edition', default='es',type=str)
    parser.add_argument('-o', '--output-directory', help='Output directory', default='./spark_2022-07_output_test', type=str)
    parser.add_argument('--num-partitions', help = "number of partitions to output",type=int, default=1)
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

    input("?")

    # start the spark session and context
    conf = SparkConf().setAppName("Wiki Regex Spark")
    spark = SparkSession.builder.getOrCreate()
    reader = spark.read
    print("> Started the Spark session...\n")

    # reading a file with spark
    reader = spark.read

    # build a schema                                                                                                                                                                                               
    struct = types.StructType().add('"REGEX_WIDE"',types.StringType(),True)
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

    df = reader.csv(files,
                    sep='\t',
                    inferSchema=False,
                    header=True,
                    mode="PERMISSIVE",
                    schema = struct)

    print(df.show(n=3, vertical=True))

    df = df.repartition(args.num_partitions)
    df = df.select('*',f.year(df.date_time).alias("year"), f.month(df.date_time).alias("month"))
    df = df.na.replace({'None': None},subset="REGEX_WIDE")
    df.show(n=5)
    print(df.count())

    # set up to track instances of NEW invocations
    my_window = Window.partitionBy("articleid").orderBy("date_time")
    df = df.withColumn("regex_prev", f.lag(df.REGEX_WIDE).over(my_window))
    df.show(n=5)

    print("# partitions".format(df.rdd.getNumPartitions()))
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
    
    df.write.parquet(args.output_dir, mode='overwrite')