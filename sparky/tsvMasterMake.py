import sys
sys.path.insert(0,"/usr/lusers/sohw/.conda/envs/wikipi_env/lib/python3.7/site-packages")
print(sys.path)

from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import lit
from pyspark.sql import types
import argparse
import glob, os, re
import csv
from pathlib import Path
from datetime import datetime
import time
import collections 
from deltas import segment_matcher, text_split

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
    regex_one_df = regex_df.select(regex_df.articleid, regex_df.namespace, regex_df.anon, regex_df.deleted, regex_df.revert, regex_df.reverteds, regex_df.revid, regex_df.date_time, f.concat_ws('_',f.year(regex_df.date_time),f.month(regex_df.date_time)).alias('YYYY_MM'),f.concat_ws(', ',*onlyRegexCols).alias('regexes'), f.concat_ws(', ',*coreDFColumn).alias('core_regexes'))

    # make sure the empty ones are None/null
    regex_one_df = regex_one_df.na.replace('',None)

    ## regex_bool and core_bool help us keep track of which revisions end in text that have PI 
    # regex_one_df = regex_one_df.select(*regex_one_df, f.when(regex_one_df.regexes.isNotNull(),1).otherwise(0).alias('regex_bool'), f.when(regex_one_df.core_regexes.isNotNull(),1).otherwise(0).alias('core_bool'))

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

    # we can just put the path in, no need to use files_l in a for-loop
    master_regex_one_df = df_regex_make(glob.glob(directory))
    print(master_regex_one_df.count())

    # Check number of partitions -- should be 1
    print('Checking number of partitions:')
    print(master_regex_one_df.rdd.getNumPartitions())

    master_regex_one_df = master_regex_one_df.orderBy('articleid')

    print("First we sort the master_regex_one_df by articleid,timestamp and add regexes_prev")
    my_window = Window.partitionBy('articleid').orderBy('date_time')
    master_regex_one_df = master_regex_one_df.withColumn('regexes_prev', f.lag(master_regex_one_df.regexes).over(my_window))
    master_regex_one_df = master_regex_one_df.withColumn('core_prev', f.lag(master_regex_one_df.core_regexes).over(my_window))

    #master_regex_one_df = master_regex_one_df.na.replace('{{EMPTYBABY}}',None)
    master_regex_one_df = master_regex_one_df.na.fill('{{EMPTYBABY}}')

    ## regexes_diff_bool, core_diff_bool keep track of # of revisions that have a new regex / 0 for no new regex, 1 for diff
    ## we can sum this for the # of revisions with difference in regex / total number of revisions
    ## regexes_diff, core_diff keep track of the actual additions (string)
    ## regexes_diff_count, core_diff_count count the number of new policy invocations from core/regexes_diff (per revision)

    master_regex_one_df = master_regex_one_df.withColumn("regexes_diff_bool", f.when(master_regex_one_df.regexes == master_regex_one_df.regexes_prev, 0).otherwise(1))
    master_regex_one_df = master_regex_one_df.withColumn("core_diff_bool", f.when(master_regex_one_df.core_regexes == master_regex_one_df.core_prev, 0).otherwise(1))

    # initialize the columns we want to fill with diff and diff_counts
    master_regex_one_df.withColumn('regexes_diff', lit('{{EMPTYBABY}}').cast(types.StringType()))
    master_regex_one_df.withColumn('core_diff', lit('{{EMPTYBABY}}').cast(types.StringType()))
    master_regex_one_df.withColumn('regexes_diff_count', lit(0).cast(types.LongType()))
    master_regex_one_df.withColumn('core_diff_count', lit(0).cast(types.LongType()))

    master_shrunken_df = master_regex_one_df.where(master_regex_one_df.regexes_diff_bool == 1)
    master_shrunken_df.orderBy(master_shrunken_df.articleid, master_shrunken_df.YYYY_MM.desc()).show(n=50)

    # Now that we have, by-revision:
    # articleid, namespace, YYYY_MM, date_time, regexes, regexes_prev, core_regex, core_prev
    ## regexes_diff_bool, core_diff_bool 
        # keep track of # of revision; that have a new regex / 0 for no new regex, 1 for diff
        ## we can sum this for the # of revisions with difference in regex / total number of revisions

    # master 
    out_filepath_master = "{}/{}_master_{}.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    #master_regex_one_df.coalesce(1).write.csv(out_filepath_master,sep='\t',mode='append',header=True)

    master_regex_one_df.orderBy(master_regex_one_df.articleid, master_regex_one_df.YYYY_MM.desc()).show(n=50)

    # shrunken master
    out_filepath_master_culled = "{}/{}_master_culled_{}.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    #master_regex_one_df.coalesce(1).write.csv(out_filepath_master_culled,sep='\t',mode='append',header=True)


    # monthly and namespace
    mn_df = master_regex_one_df.repartition("YYYY_MM","namespace")
    mn_df = mn_df.groupBy("YYYY_MM","namespace").agg(f.count("*").alias("num_revs"), f.sum("regexes_diff_bool").alias("num_revs_with_regex_diff"), f.sum("core_diff_bool").alias("num_revs_with_core_diff"))
    mn_df.orderBy(mn_df.YYYY_MM.desc()).show(n=50)

    out_filepath_monthly_namespace = "{}/{}_monthly-namespace_{}.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    #mn_df.coalesce(1).write.csv(out_filepath_monthly_namespace,sep='\t',mode='append',header=True) 

    # monthly
    m_df = master_regex_one_df.repartition("YYYY_MM")
    m_df = m_df.groupBy("YYYY_MM").agg(f.count("*").alias("num_revs"), f.sum("regexes_diff_bool").alias("num_revs_with_regex_diff"), f.sum("core_diff_bool").alias("num_revs_with_core_diff"))
    m_df.orderBy(m_df.YYYY_MM.desc()).show(n=50)

    out_filepath_monthly = "{}/{}_monthly_{}.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    #m_df.coalesce(1).write.csv(out_filepath_monthly,sep='\t',mode='append',header=True)


    # TODO IN THE NEXT SCRIPT
    ## regexes_diff, core_diff 
        # keep track of the actual additions (string)
    ## regexes_diff_count, core_diff_count
        # count the number of new policy invocations from core/regexes_diff (per revision)


    print("Find the output here: \nmaster:{}\nculled master:{}\nmonthly:{}\nmonthly_namespace:{}".format(out_filepath_master, out_filepath_master_culled, out_filepath_monthly,out_filepath_monthly_namespace))

    print("\n\n---Ending Spark Session and Context ---\n\n")
    spark.stop()

    # Input that to calculate diffs
    '''
    master_regex_one_df.foreach(diff_find)
    # we now have the diffs for each; we know this is BY ARTICLE because of the window thing we did earlier...

    #master_regex_one_df.orderBy('articleid','YYYY_MM','date_time').show(n=100)

    print("\n\n\n")

    master_regex_one_df.select(master_regex_one_df.articleid,master_regex_one_df.YYYY_MM,master_regex_one_df.date_time,master_regex_one_df.regexes,master_regex_one_df.regexes_prev,master_regex_one_df.regexes_diff,master_regex_one_df.regexes_diff_count).orderBy('articleid','YYYY_MM','date_time').show(n=100)

    print("Partitions right now: {}".format(master_regex_one_df.rdd.getNumPartitions()))

    print("Now we're ready to process the data (MONTHLY SMOOSH)")

    #print("Repartitioning articleid,YYYY_MM:")
    #rp_df = master_regex_one_df.repartition("articleid","YYYY_MM")
    #print(rp_df.rdd.getNumPartitions())

    print("Time to process into monthly now, I guess...")
    print("\n\n---Ending Spark Session and Context ---\n\n")

    print("--- %s seconds ---" % (time.time() - start_time))
    spark.stop()

    # Now that we have, by-revision:
    # articleid, namespace, YYYY_MM, date_time, regexes, regexes_prev, core_regex, core_prev
    ## regexes_diff_bool, core_diff_bool 
        # keep track of # of revision; that have a new regex / 0 for no new regex, 1 for diff
        ## we can sum this for the # of revisions with difference in regex / total number of revisions
    ## regexes_diff, core_diff 
        # keep track of the actual additions (string)
    ## regexes_diff_count, core_diff_count
        # count the number of new policy invocations from core/regexes_diff (per revision)
    
    # Smooth into months
    print("Repartitioning articleid,YYYY_MM:")
    rp_df = master_regex_one_df.repartition("YYYY_MM","namespace")
    # groupBy YYYY_MM ...
    # sum up the regexes_diff_bool --> num_revs_with_regex_diff, core_diff_bool --> num_revs_with_core_diff
    # concatenate all of the strings of regexes_diff and core_diff that are not empty --> regexes_diff_monthly, core_diff_monthly
    # sum up the regexes_diff_count, core_diff_count --> regexes_diff_count_monthly, core_diff_count_monthly
        # this is the number of new policy invocations in that month
    # f.count(*) --? num_revs

    rp_df = rp_df.groupBy("YYYY_MM","namespace").agg( f.count("*").alias("num_revs"), f.sum("regexes_diff_bool").alias("num_revs_with_regex_diff"), f.sum("core_diff_bool").alias("num_revs_with_core_diff"), f.sum("regexes_diff_count").alias("regexes_diff_count_monthly"), f.sum("core_diff_count").alias("core_diff_count_monthly"), f.concat_ws(", ", f.collect_list(rp_df.regexes_diff)).alias("regexes_diff_monthly"),  f.concat_ws(", ", f.collect_list(rp_df.core_diff)).alias("core_diff_monthly"))

    #TODO concat_ws for regexes/core_diff_monthly CONDITIONAL --> WHEN NOT EMPTY
    #TODO {{EMPTYBABY}} CONSISTENCY


    rp_df = rp_df


    #TODO FIGURE OUT WHAT FILES ARE TO BE OUTPUTTED. will probably have multiple dfs

    out_filepath = "{}/{}{}.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    rp_df.coalesce(1).write.csv(out_filepath,sep='\t',mode='append',header=True)
    
    print("Find the output here: {}".format(out_filepath))

    print("\n\n---Ending Spark Session and Context ---\n\n")
    spark.stop()
    '''