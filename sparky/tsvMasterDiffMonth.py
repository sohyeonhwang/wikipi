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
import collections 

start_time = time.time()

def parse_args():
    parser = argparse.ArgumentParser(description='Create a dataset.')
    parser.add_argument('-i', '--input', help='Path for directory of wikiq tsv outputs', required=True, type=str)
    parser.add_argument('--lang', help='Specify which language edition', default='es',type=str)
    parser.add_argument('-o', '--output-directory', help='Output directory', default='./tsvCrunchOutput', type=str)
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
    #metaColumns = struct.fieldNames()

    # new dataframe of the regex columns
    regexDFColumns = [c for c in input_df.columns if c[0].isdigit()]
    regexDFColumns.append("revid")
    regexDFColumns.append("date_time")
    regexDFColumns.append("articleid")
    regexDFColumns.append("namespace")
    regexDFColumns.append("anon")
    regexDFColumns.append("deleted")
    regexDFColumns.append("revert")
    regexDFColumns.append("reverteds")
    regex_df = input_df.na.replace('None',None).select(*regexDFColumns)
    #regex_df.show(n=5, vertical=True)

    # combine the regex columns into one column, if not None/null
    # this has: revid, article_id, date/time, regexes, core_regexes, regex_bool, core_bool
    onlyRegexCols = [c for c in regex_df.columns if c[0].isdigit()]
    coreDFColumn = findCoreColumns(onlyRegexCols)
    regex_one_df = regex_df.select(regex_df.articleid, regex_df.namespace, regex_df.anon, regex_df.deleted, regex_df.revert, regex_df.reverteds, regex_df.revid, regex_df.date_time, f.concat_ws('_',f.year(regex_df.date_time),f.month(regex_df.date_time)).alias('YYYY-MM'),f.concat_ws('| ',*onlyRegexCols).alias('regexes'), f.concat_ws('| ',*coreDFColumn).alias('core_regexes'))

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
                        mode="PERMISSIVE",
                        quote="")
    #tsv2df = tsv2df.repartition(args.num_partitions)

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

def diff_find(current,prev):
    # we want to write a function that will find the difference between new rev and old rev
    # there are 3 possibilities
        # no difference
        # new has MORE REGEXES than old
        # new has FEWER REGEXES than old
        # the third case is the most complicated; this usually means the page has been edited s.t. content has been removed, removing the regex
            # e.g. a revert
            # in the case of FEWER REGEXES, we want to check variables: revert and reverteds

    current_list = current.split("| ")
    prev_list = prev.split("| ")

    # the lists are the same -- our simplest case and i suspect what will be the case most of the time
    if current_list == prev_list:
        diff = None
    
    # something is different about the lists...
    else:
        #num_current = len(current_list)
        #num_prev = len(prev_list)
        current_c = collections.Counter(current_list)
        prev_c = collections.Counter(prev_list)

        #TODO deltas 
        diff = current_c + prev_c

        # we throw away the starting equal

        # logic for assuming what is new

    return diff

if __name__ == "__main__":
    args = parse_args()

    # checking args and retrieving inputs
    print("INPUT:\t{}".format(args.input))
    print(" LANG:\t{}".format(args.lang))
    print("O_DIR:\t{}".format(args.output_directory))
    print(" O_FN:\t{}".format(args.output_filename))

    if not os.path.isdir(args.output_directory):
        os.mkdir(args.output_directory)

    # e.g. /gscratch/comdata/users/sohw/wikipi/wikiq_runs/output_samples/tsvSampleInputs
    directory = "{}/{}wiki/*".format(args.input,args.lang)
    print("INPUT PATH:{}".format(directory))

    files = glob.glob(directory)

    # print(files)
    files_l = [os.path.abspath(p) for p in files]
    print("Number of tsvs to process: {}\n".format(len(files_l)))

    # start the spark session and context
    conf = SparkConf().setAppName("wiki regex spark processing")
    spark = SparkSession.builder.getOrCreate()
    reader = spark.read
    print("Started the Spark session...\n")

    #we can just put the path in, no need to use files_l in a for-loop
    #just glob it (different from just one: df_1 = df_regex_make(files_l[0]))
    master_regex_one_df = df_regex_make(glob.glob(directory))

    # Compared just one file and regex make of directory
    #print('Checking that the df from path/* is indeed different from one file input...')
    #print("glob/*:{}\none_file:{}".format(master_regex_one_df.count(),df_1.count()))
    #print(master_regex_one_df.describe().show())
    #print(df_1.describe().show())

    # Check number of partitions -- should be 1
    print('Checking number of partitions - should be 1 b/c df.repartition(1) in df_regex_make')
    print(master_regex_one_df.rdd.getNumPartitions())
    master_regex_one_df = master_regex_one_df.repartition(args.num_partitions)
    # print(master_regex_one_df.rdd.getNumPartitions())

    #print("Columns of the processed dataframe:\n")
    #for c in master_regex_one_df.columns:
    #    print("\t{}".format(c))    

    print("--- %s seconds ---" % (time.time() - start_time))

    #master_regex_one_df.orderBy('articleid').show(n=3,vertical=True)
    #master_regex_one_df.orderBy(master_regex_one_df.articleid.asc()).show(n=3,vertical=True)
    master_regex_one_df = master_regex_one_df.orderBy('articleid')

    print("First we sort the master_regex_one_df by articleid,timestamp and add prev_rev_regex")
    my_window = Window.partitionBy('articleid').orderBy('date_time')
    master_regex_one_df = master_regex_one_df.withColumn('regexes_prev', f.lag(master_regex_one_df.regexes).over(my_window))
    master_regex_one_df = master_regex_one_df.withColumn('core_prev', f.lag(master_regex_one_df.core_regexes).over(my_window))

    ## diff_bool, diff_core_bool keep track of # of revisions that have a new regex
    master_regex_one_df = master_regex_one_df.withColumn("regexes_diff_bool", f.when(master_regex_one_df.regexes == master_regex_one_df.prev_rev_regex, 0).otherwise(1))
    master_regex_one_df = master_regex_one_df.withColumn("core_diff_bool", f.when(master_regex_one_df.core_regexes == master_regex_one_df.prev_rev_core, 0).otherwise(1))

    '''
    ##TODO diff_regex diff_core keep track of the actual additions (string)
    # current = regexes, prev = prev_rev_regex
    diff_regex, diff_regex_count = diff_find(master_regex_one_df.regexes,master_regex_one_df.prev_rev_regex)
    diff_core, diff_core_count = diff_find(master_regex_one_df.core_regexes,master_regex_one_df.prev_rev_core)

    master_regex_one_df = master_regex_one_df.withColumn('regexes_diff',f.when(f.isnull(diff_regex), None).otherwise(diff_regex))
    master_regex_one_df = master_regex_one_df.withColumn('core_diff', f.when(f.isnull(diff_core), None).otherwise(diff_core))

    ##TODO diff_regex_count diff_core_count counts the number of new policy invocations
    master_regex_one_df = master_regex_one_df.withColumn('regexes_diff_count')
    master_regex_one_df = master_regex_one_df.withColumn('core_diff_count')
    '''

    master_regex_one_df.orderBy('articleid','YYYY-MM','date_time').show(n=100)

    print("Partitions right now: {}".format(master_regex_one_df.rdd.getNumPartitions()))

    print("Now we're ready to process the data.")

    print("Repartitioning articleid,YYYY-MM:")
    #rp_df = master_regex_one_df.repartition("articleid","YYYY-MM")
    #print(rp_df.rdd.getNumPartitions())

    out_filepath = "{}/{}{}.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    print("Find the output here: {}".format(out_filepath))

    '''
    master_regex_one_df.write.partitionBy("articleid","YYYY-MM").mode("overwrite").csv(out_filepath,sep='\t',header=True)

    # We now read from the partitioned data articleid, YYYY-MM
    partitioned_df = spark.read.option("basePath","{}/".format(out_filepath)).csv("{}/articleid=*/YYYY-MM=*".format(out_filepath))
    print(partitioned_df.rdd.getNumPartitions())
    '''

    print("Time to process the diffs now, I guess...")
    print("\n\n---Ending Spark Session and Context ---\n\n")
    spark.stop()
'''
    # now have articleid, namespace, YYYY-MM, date_time, regexes, prev_rev_regex, diff
    # 2.2 - monthly smoosh, monthly_df
        # want in monthly_df:
        # YYYY-MM, namespace, regexes_start, regexes_end, not_count_diff(next_month_start-month_start)
        # core_regexes_start, core_regexes_end, not_count_diff(next_month_start - month_start)
        # count(revisions), count(revs_with_diff), count(revs_with_core_diff)
    # forEachPartition:
        # A. cumulatively cat diff, core_diff (# of policy invocations may be different from # rev with core_diff)
        # B. end - start
    #

    # 2.2 TODO ALTERNATIVE BY MONTH DIFF
    # from the partitioned data
    # get first and last of each month for each article
    # so each articleid will have two rows for every month, that means 24/year, which means ~410 per article...

    # new_df --> articleID, YYYY-MM, namespace, regex_start, regex_end, core_start, core_end
    # one row per articleID + YYYY-MM combo
    # in new_df: calculate the diff for each articleid, month (new column diff)
    # diff is a COUNT. for YYYYMM would be YYYYMM+1(regex_start) - YYYYMM(regex_start)
    # first month starts at 0; last month is regex_end - regex_start
    # core_diff follows same logic, just for the core
    print("\nGenerate new column of diff, core_diff")

    # smoosh into year/month (no more articleid)
    # and then we add up all the diffs (groupBy -->YYYY-MM, namespace. so YYYY-MM, diff)
    # so a df that is: one row per YYYY-MM + namespace combo.
    # smooth into just YYYY-MM, diff info (ignore namespace) in R




    # 3 TODO F1. needs the by-rev
    # YYYY-MM, namespace, regex_diff, core_diff, refex_diff_count, core_diff_count
    # from the partitionBy(articleid, YYYY-MM) situation, we want to get:
    # the count of revisions
    # the count of revisions with policy invocation (there is a diff adding), or core policy invocation
    # per month 
    # active editor data is elsewhere.

    # MONTHLY BASIC DATA. of revisions with policy invoked, how many had core policy invocations by-month and cumul
    # count revisions with core policy invocation
    # count revisions

    # 4 TODO F4. YYYY-MM, namespace, regex_diff (not count)
    # we want to have the new policy invocations in a given month, so export that or use the exported file from F1
    # going to have to write a separate script that goes through the regex_diff of a month and checks ILL status


    print("We have now built the columns with the diffs (current, prev).")
    print("--- %s seconds ---" % (time.time() - start_time))

    #TODO FIGURE OUT WHAT FILES ARE TO BE OUTPUTTED. will probably have multiple dfs
    #TODO double-check the output format based on the partitioning....
    out_filepath = "{}/{}{}.tsv".format(args.output_dir,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    master_regex_one_df.coalesce(1).write.csv(out_filepath,sep='\t',mode='append',header=True)


    print("\n\n---Ending Spark Session and Context ---\n\n")
    spark.stop()
    '''
