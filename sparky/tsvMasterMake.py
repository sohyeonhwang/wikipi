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
import glob, os
import csv
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

def replace_wp_wikipedia(col_name,temp):
    regexp = r"(WP|wp|Wikipedia|wikipedia|WIKIPEDIA|Wikipédia|wikipédia|WIKIPÉDIA)($|([^:\s]))"
    return f.regexp_replace(col_name, regexp, temp)

def multi_replace_wps(col_names):
    # Adapted from
    # https://medium.com/@mrpowers/performing-operations-on-multiple-columns-in-a-pyspark-dataframe-36e97896c378
    def inner(df):
        for col_name in col_names:
            print(col_name)
            temp = col_name
            df = df.withColumn( col_name , f.when(df[col_name].isNotNull(), replace_wp_wikipedia(col_name,temp)) )
        return df
    return inner

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

    replaced_df = multi_replace_wps(onlyRegexCols)(regex_df)

    #test_df.select(regex_df.revid, regex_df.date_time, f.year(regex_df.date_time).alias("year"), f.month(regex_df.date_time).alias('month'),f.concat_ws(', ',*onlyRegexCols).alias('regexes'), f.concat_ws(', ',*coreDFColumn).alias('core_regexes')).show(n=50, truncate=200)

    #print("If we didn't do the replace stuff:")
    #regex_df.select(regex_df.revid, regex_df.date_time, f.year(regex_df.date_time).alias("year"), f.month(regex_df.date_time).alias('month'),f.concat_ws(', ',*onlyRegexCols).alias('regexes'), f.concat_ws(', ',*coreDFColumn).alias('core_regexes')).show(n=50, truncate =200)

    regex_one_df = replaced_df.select(regex_df.articleid, regex_df.namespace, regex_df.anon, regex_df.deleted, regex_df.revert, regex_df.reverteds, regex_df.revid, regex_df.date_time, f.year(regex_df.date_time).alias("year"), f.month(regex_df.date_time).alias('month'),f.concat_ws(', ',*onlyRegexCols).alias('regexes'), f.concat_ws(', ',*coreDFColumn).alias('core_regexes'))

    # if you don't want to use the replaced version, use this:
    # regex_one_df = regex_df.select(regex_df.articleid, regex_df.namespace, regex_df.anon, regex_df.deleted, regex_df.revert, regex_df.reverteds, regex_df.revid, regex_df.date_time, f.year(regex_df.date_time).alias("year"), f.month(regex_df.date_time).alias('month'),f.concat_ws(', ',*onlyRegexCols).alias('regexes'), f.concat_ws(', ',*coreDFColumn).alias('core_regexes'))

    # make again sure the empty ones are None/null
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
    print("master_regex_one_df.count() --> {}".format(master_regex_one_df.count()))

    # Check number of partitions -- should be 1
    print('Checking number of partitions:')
    print(master_regex_one_df.rdd.getNumPartitions())

    master_regex_one_df = master_regex_one_df.orderBy('articleid')

    my_window = Window.partitionBy('articleid').orderBy('date_time')
    master_regex_one_df = master_regex_one_df.withColumn('regexes_prev', f.lag(master_regex_one_df.regexes).over(my_window))
    master_regex_one_df = master_regex_one_df.withColumn('core_prev', f.lag(master_regex_one_df.core_regexes).over(my_window))

    #master_regex_one_df = master_regex_one_df.na.replace('{{EMPTYBABY}}',None)
    master_regex_one_df = master_regex_one_df.na.fill('{{EMPTYBABY}}')

    ## regexes_diff_bool, core_diff_bool keep track of # of revisions that have a new regex / 0 for no new regex, 1 for diff
    ## we can sum this for the # of revisions with difference in regex / total number of revisions

    master_regex_one_df = master_regex_one_df.withColumn("regexes_diff_bool", f.when(master_regex_one_df.regexes == master_regex_one_df.regexes_prev, 0).otherwise(1))
    master_regex_one_df = master_regex_one_df.withColumn("core_diff_bool", f.when(master_regex_one_df.core_regexes == master_regex_one_df.core_prev, 0).otherwise(1))

    # initialize the columns we want to fill with diff and diff_counts
    ## regexes_diff, core_diff keep track of the actual additions (string)
    ## regexes_diff_count, core_diff_count count the number of new policy invocations from core/regexes_diff (per revision)
    master_regex_one_df.withColumn('regexes_diff', lit('{{EMPTYBABY}}').cast(types.StringType()))
    master_regex_one_df.withColumn('core_diff', lit('{{EMPTYBABY}}').cast(types.StringType()))
    master_regex_one_df.withColumn('regexes_diff_count', lit(0).cast(types.LongType()))
    master_regex_one_df.withColumn('core_diff_count', lit(0).cast(types.LongType()))

    # Now that we have, by-revision:
    # articleid, namespace, year, month, date_time, regexes, regexes_prev, core_regex, core_prev
    ## regexes_diff_bool, core_diff_bool 
        # keep track of # of revision; that have a new regex / 0 for no new regex, 1 for diff
        ## we can sum this for the # of revisions with difference in regex / total number of revisions

    # make the smaller version to be outputted
    master_regex_one_df = master_regex_one_df.repartition(160)
    mid_time1 = time.time()
    print("Number of partitions of master_regex_one_df: {}".format(master_regex_one_df.rdd.getNumPartitions()))
    master_shrunken_df = master_regex_one_df.where('regexes_diff_bool == 1 or core_diff_bool == 1')

    print("--- %s seconds ---" % (time.time() - mid_time1))

    print('these two should be these same, the summing of regexes_diff_bool and core_diff_bool:')
    # TEST - these two should be the same (if not 0):
    print("master:")
    master_regex_one_df.groupBy("year","month").agg(f.sum("regexes_diff_bool").alias("num_revs_with_regex_diff"), f.sum("core_diff_bool").alias("num_revs_with_core_diff")).orderBy(master_regex_one_df.year, master_regex_one_df.month).show(n=30)
    print("filtered:")
    master_shrunken_df.groupBy("year","month").agg(f.sum("regexes_diff_bool").alias("num_revs_with_regex_diff"), f.sum("core_diff_bool").alias("num_revs_with_core_diff")).orderBy(master_shrunken_df.year, master_shrunken_df.month).show(n=30)

    print('\n\n\n')


    # MASTER 
    #TODO See if we can export the master_regex_one_df file actually
    print("Preview master_regex_one_df: ")
    master_regex_one_df.orderBy(master_regex_one_df.articleid.asc_nulls_first(), master_regex_one_df.year, master_regex_one_df.month, master_regex_one_df.date_time).show(n=10)
    
    out_filepath_master = "{}/{}_master_{}.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    #master_regex_one_df.coalesce(1).write.csv(out_filepath_master,sep='\t',mode='append',header=True)

    # MASTER - FILTERED ROWS
    print("Preview FILTERED (ROWS) master_regex_one_df: ")
    master_shrunken_df = master_shrunken_df.repartition(100)
    master_shrunken_df.orderBy(master_shrunken_df.articleid.asc_nulls_first(), master_shrunken_df.year, master_shrunken_df.month, master_regex_one_df.date_time).show(n=10)
    print("master_shrunken_df.count() --> {}".format(master_shrunken_df.count()))

    out_filepath_master_filtered = "{}/{}_master_filtered_{}.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    master_shrunken_df.coalesce(1).write.csv(out_filepath_master_filtered,sep='\t',mode='append',header=True)

    # MONTHLY + NAMESPACE
    print("Preview monthly + namespace df: ")
    mn_df = master_regex_one_df.repartition("year","month","namespace")
    mn_df = mn_df.groupBy("year","month","namespace").agg(f.count("*").alias("num_revs"), f.sum("regexes_diff_bool").alias("num_revs_with_regex_diff"), f.sum("core_diff_bool").alias("num_revs_with_core_diff")).orderBy(mn_df.year, mn_df.month)
    mn_df.orderBy(mn_df.year.desc(), mn_df.month.asc()).show(n=50)

    out_filepath_monthly_namespace = "{}/{}_monthly-namespace_{}.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    mn_df.coalesce(1).write.csv(out_filepath_monthly_namespace,sep='\t',mode='append',header=True) 

    # MONTHLY
    print("Preview monthly df: ")
    m_df = master_regex_one_df.repartition("year","month")
    m_df = m_df.groupBy("year","month").agg(f.count("*").alias("num_revs"), f.sum("regexes_diff_bool").alias("num_revs_with_regex_diff"), f.sum("core_diff_bool").alias("num_revs_with_core_diff")).orderBy(m_df.year, m_df.month)
    m_df.orderBy(m_df.year.desc(),m_df.month.asc()).show(n=50)

    out_filepath_monthly = "{}/{}_monthly_{}.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    m_df.coalesce(1).write.csv(out_filepath_monthly,sep='\t',mode='append',header=True)

    print("Find the output here: \nmaster:{}\nfiltered (row) master:{}\nmonthly:{}\nmonthly_namespace:{}".format(out_filepath_master, out_filepath_master_filtered, out_filepath_monthly,out_filepath_monthly_namespace))

    print("\n\n---Ending Spark Session and Context ---\n\n")
    spark.stop()
    print("--- %s seconds ---" % (time.time() - start_time))

    # TO DO IN THE NEXT SCRIPT
    ## regexes_diff, core_diff 
        # keep track of the actual additions (string)
    ## regexes_diff_count, core_diff_count
        # count the number of new policy invocations from core/regexes_diff (per revision)
