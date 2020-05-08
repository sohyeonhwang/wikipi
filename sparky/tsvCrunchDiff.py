import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql import types
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

def findCoreColumns(onlyRegexCols):
    #CORE COLUMNS
    #ENGLISH:
    #SPANISH:	53_WIKIPEDIA:PUNTO_DE_VISTA_NEUTRAL, 69_WIKIPEDIA:WIKIPEDIA_NO_ES_UNA_FUENTE_PRIMARIA, 64_WIKIPEDIA:VERIFICABILIDAD
    #FRENCH: 	26_WIKIPÉDIA:NEUTRALITÉ_DE_POINT_DE_VUE, 19_WIKIPÉDIA:TRAVAUX_INÉDITS, 21_WIKIPÉDIA:VÉRIFIABILITÉ
    #GERMAN:
    #JAPANESE:	14_WIKIPEDIA:中立的な観点,16_WIKIPEDIA:独自研究は載せない, 15_WIKIPEDIA:検証可能性
    
    if "53_WIKIPEDIA:PUNTO_DE_VISTA_NEUTRAL" in onlyRegexCols:
        coreDFColumn = [c for c in onlyRegexCols if (c[:2]==str(53) or c[:2]==str(69) or c[:2]==str(64))]
    elif "26_WIKIPÉDIA:NEUTRALITÉ_DE_POINT_DE_VUE" in onlyRegexCols:
        coreDFColumn = [c for c in onlyRegexCols if (c[:2]==str(26) or c[:2]==str(19) or c[:2]==str(21))]
    else:
        coreDFColumn = [c for c in onlyRegexCols if (c[:2]==str(14) or c[:2]==str(15) or c[:2]==str(16))]

    return coreDFColumn

def diff_find(current,prev):
    # we want to write a function that will find the difference between new rev and old rev
    # there are 3 possibilities
        # no difference
        # new has MORE REGEXES than old
        # new has FEWER REGEXES than old
        # the third case is the most complicated; this usually means the page has been edited s.t. content has been removed, removing the regex
            # e.g. a revert
            # in the case of FEWER REGEXES, we want to check variables: revert and reverteds
    diff = ""

    current_list = current.split("| ")
    prev_list = prev.split("| ")

    if current_list == prev_list:
        diff = None
    else:
        num_current = len(current_list)
        num_prev = len(prev_list)

        




    return diff

if __name__ == "__main__":
    args = parse_args()

    # checking args and retrieving inputs
    print("INPUT:\t{}".format(args.input))
    print("LANG ED:\t{}".format(args.lang))
    print("O_DIR:\t{}".format(args.output_directory))
    print("O_FN:\t{}".format(args.output_filename))

    if not os.path.isdir(args.output_dir):
        os.mkdir(args.output_dir)

    # input should just be one file now, the master_tsv for a lang
    # im guessing it'll be something like:
    # /gscratch/comdata/users/sohw/wikipi/wikipi_repo/sparky/tsvCrunchDiffInput
    # with subdirectories for each lang edition

    # e.g. /gscratch/comdata/users/sohw/wikipi/wikiq_runs/output_samples/tsvSampleInputs
    directory = "{}/{}wiki/*.tsv".format(args.input,args.lang)
    print("INPUT PATH:{}".format(directory))

    files = glob.glob(directory)
    files = [os.path.abspath(p) for p in files]
    print("The master tsv to Spark: {}\n".format(len(files)))

    master_tsv = files[0]

    # start the spark session and context
    conf = SparkConf().setAppName("wiki regex spark processing")
    spark = SparkSession.builder.getOrCreate()
    reader = spark.read
    print("Started the Spark session...\n")

    # read the tsv into a dataframe and build the proper schema
    tsv2df = reader.csv(master_tsv,
                        sep="\t",
                        inferSchema=False,
                        header=True,
                        mode="PERMISSIVE")
    tsv2df = tsv2df.repartition(args.num_partitions)

    struct = types.StructType().add("articleid",types.StringType(),True)
    struct = struct.add("revid", types.LongType(), True)
    struct = struct.add("anon", types.LongType(), True)
    struct = struct.add("namespace",types.LongType(),True)
    struct = struct.add("deleted",types.BooleanType(), True)
    struct = struct.add("revert", types.BooleanType(), True)
    struct = struct.add("reverteds", types.StringType(), True)
    struct = struct.add("date_time",types.TimestampType(), True)
    struct = struct.add("year_month",types.StringType(), True)
    struct = struct.add("regexes",types.StringType(), True)
    struct = struct.add("core_regexes",types.StringType(), True)
    struct = struct.add("regex_bool",types.StringType(), True)
    struct = struct.add("core_bool",types.StringType(), True)
    print("Finished reading the tsv input into dataframe.")

    #TODO DIFFS 
    tsv2df = tsv2df.orderBy('articleid')
    my_window = Window.partitionBy('articleid').orderBy('date_time')

    tsv2df = tsv2df.withColumn('prev_rev_regex', f.lag(tsv2df.regexes).over(my_window))
    #TODO
    # we want to write a function that will find the difference between new rev and old rev
    # current = regexes
    # prev = prev_rev_regex
    diff = diff_find(tsv2df.regexes,tsv2df.prev_rev_regex)

    tsv2df = tsv2df.withColumn('diff',f.when(f.isnull("[THERE IS NO DIFFERENCE]"), 0).otherwise("[THE DIFF]"))

    print("We have now built the columns with the diffs (current, prev).")
    print("--- %s seconds ---" % (time.time() - start_time))


    #TODO now we smoosh to months




    # we should now have a disgustingly large dataframe of all the TSVS
    # output that to do other things in a different script aka the groupBy article, month Squish

    #out_filepath = "{}/{}{}.tsv".format(args.output_dir,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    #master_regex_one_df.coalesce(1).write.csv(out_filepath,sep='\t',header=True)

    tsv2df.show(n=10,vertical=True)
    print("\n\nEnding Spark Session and Context\n\n")
    spark.stop()