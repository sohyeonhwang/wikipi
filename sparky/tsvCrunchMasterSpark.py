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

def df_structurize(input_df, struct):
    # metadata columns
    #metaColumns = struct.fieldNames()
    #meta_df = input_df.select(*metaColumns)
    #meta_df.orderBy("articleid").show()

    # new dataframe of the regex columns
    regexDFColumns = [c for c in input_df.columns if c[0].isdigit()]
    regexDFColumns.append("revid")
    regexDFColumns.append("date_time")
    regexDFColumns.append("articleid")
    regex_df = input_df.na.replace('None',None).select(*regexDFColumns)
    #regex_df.show(n=5, vertical=True)

    # combine the regex columns into one column, if not None/null
    # this has: revid, article_id, date/time, regexes, core_regexes, regex_bool, core_bool
    onlyRegexCols = [c for c in regex_df.columns if c[0].isdigit()]
    coreDFColumn = findCoreColumns(onlyRegexCols)
    regex_one_df = regex_df.select(regex_df.articleid, regex_df.namespace, regex_df.anon, regex_df.deleted, regex_df.revert, regex_df.reverteds, regex_df.revid, regex_df.date_time, f.concat_ws('_',f.year(regex_df.date_time),f.month(regex_df.date_time)).alias('year_month'),f.concat_ws('| ',*onlyRegexCols).alias('regexes'), f.concat_ws('| ',*coreDFColumn).alias('core_regexes'))

    # make sure the empty ones are None/null
    regex_one_df = regex_one_df.na.replace('',None)

    regex_one_df = regex_one_df.select(*regex_one_df, f.when(regex_one_df.regexes.isNotNull(),1).otherwise(0).alias('regex_bool'), f.when(regex_one_df.core_regexes.isNotNull(),1).otherwise(0).alias('core_bool'))

    #regex_one_df.show(n=5, vertical=True)

    return regex_one_df

def df_regex_make(wikiqtsv):
    # make wikiq tsv into a dataframe
    tsv2df = reader.csv(wikiqtsv,
                        sep="\t",
                        inferSchema=False,
                        header=True,
                        mode="PERMISSIVE")
    tsv2df = tsv2df.repartition(args.num_partitions)

    # basic structure
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

    # structure the df to get the def with columns of metadata and regexes
    regex_one_df = df_structurize(tsv2df,struct)

    return regex_one_df


if __name__ == "__main__":
    args = parse_args()

    # checking args and retrieving inputs
    print("INPUT:\t{}".format(args.input))
    print("LANG ED:\t{}".format(args.lang))
    print("O_DIR:\t{}".format(args.output_directory))
    print("O_FN:\t{}".format(args.output_filename))

    if not os.path.isdir(args.output_dir):
        os.mkdir(args.output_dir)

    # e.g. /gscratch/comdata/users/sohw/wikipi/wikiq_runs/output_samples/tsvSampleInputs
    directory = "{}/{}wiki/*.tsv".format(args.input,args.lang)
    print("INPUT PATH:{}".format(directory))

    files = glob.glob(directory)
    print(files)
    files = [os.path.abspath(p) for p in files]
    print("Number of tsvs to process: {}\n".format(len(files)))

    # start the spark session and context
    conf = SparkConf().setAppName("wiki regex spark processing")
    spark = SparkSession.builder.getOrCreate()
    reader = spark.read
    print("Started the Spark session...\n")

    #TODO - OPTION B
    # we build the master as we lightly process through the files into a df
    # initialize master 
    print("Starting TSV processing")
    master_regex_one_df = ""
    for tsv_f in files:
        print("Looking at: {}".format(tsv_f))

        # we use the first one as the starter
        # this if statement should ALWAYS run JUST ONCE, at the beginning of the loop
        if tsv_f == files[0]:
            print("Looking at: {} ; FIRST FILE ONLY DOUBLE PRINT SANITY CHECK\n".format(tsv_f))
            
            # df_regex_make generates the df with regexes smooshed into one column
            # df_regex_make relies on df_structurize, which formats fiddles with the column structures
            master_regex_one_df = df_regex_make(tsv_f)

            # df_monthly_make takes the df_regex_make() output and groups everything by month

        else:
            ro_df2 = df_regex_make(tsv_f)

            # choose your poison
            #master_df = master_df.unionByName(df2)
            master_regex_one_df = master_regex_one_df.unionByName(ro_df2)

    print("TSV processing done.")
    print("--- %s seconds ---" % (time.time() - start_time))
    # we should now have a disgustingly large dataframe of all the TSVS
    # output that to do other things in a different script aka the groupBy article, month Squish

    #out_filepath = "{}/{}{}.tsv".format(args.output_dir,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    #master_regex_one_df.coalesce(1).write.csv(out_filepath,sep='\t',mode='append',header=True)

    master_regex_one_df.show(n=10,vertical=True)
    print("\n\n---Ending Spark Session and Context ---\n\n")
    spark.stop()