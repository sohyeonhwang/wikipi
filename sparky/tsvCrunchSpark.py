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
from datetime import date
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

def df_structurize(input_df, struct):
    # metadata columns
    metaColumns = struct.fieldNames()
    meta_df = input_df.select(*metaColumns)
    #meta_df.orderBy("articleid").show()

    # dataframe of the regex columns
    regexDFColumns = [c for c in input_df.columns if c[0].isdigit()]
    regexDFColumns.append("revid")
    regexDFColumns.append("date_time")
    regexDFColumns.append("articleid")
    regex_df = input_df.na.replace('None',None).select(*regexDFColumns)
    #regex_df.show(vertical=True)

    # combine the regex columns into one column, if not None/null
    # this has: revid, article_id, date/time, regexes
    #onlyRegexCols = [c for c in regex_df.columns if c[0].isdigit()]
    #regexes_revid_df = regex_df.select(regex_df.revid,regex_df.articleid, regex_df.date_time,f.concat_ws(', ',*onlyRegexCols).alias("REGEXES"))
    #regexes_revid_df.show(vertical=True)

    return meta_df, regex_df

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
    meta_df, regex_df = df_structurize(tsv2df,struct)
    meta_df = meta_df

    return regex_df

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

def df_monthly_make(regex_df):
    onlyRegexCols = [c for c in regex_df.columns if c[0].isdigit()]
    coreDFColumn = findCoreColumns(onlyRegexCols)

    monthly_regex_df = regex_df.select(regex_df.revid, f.concat_ws('_',f.year(regex_df.date_time),f.month(regex_df.date_time)).alias('year_month'),f.concat_ws(', ',*coreDFColumn).alias('core_regex'))
    monthly_regex_df = monthly_regex_df.na.replace('',None)
    monthly_regex_df = monthly_regex_df.select(*monthly_regex_df,f.when(monthly_regex_df.core_regex.isNotNull(),1).otherwise(0).alias('core_policy_invoked'))

    # counts the number of revisions with core policy invocation in year_month
    monthly_core_count_df = monthly_regex_df.groupBy('year_month').sum('core_policy_invoked')
    # counts the number of revisions in year_month
    monthly_revn_count_df = monthly_regex_df.groupBy('year_month').count()

    monthly_joined_df = monthly_revn_count_df.join(monthly_core_count_df, on=['year_month'],how='left')

    return monthly_joined_df

def combine_dfs(mdf_list):
    # starter df
    combined_df = mdf_list[0]
    print("STARTER DF")
    combined_df.show(n=5,vertical=True)

    # since 0 is the starter, don't want to double count. from the next one, we iterate over and add
    for i in range(1,len(mdf_list)):
        df2 = mdf_list[i]
        # rename columns in df2
        df2 = df2.withColumnRenamed("count","count2")
        df2 = df2.withColumnRenamed("sum(core_policy_invoked)","sum(core_policy_invoked)2")
        print("THE DF TO ADD TO CURRENT STARTER")
        df2.show(n=5,vertical=True)

        # join and fillna as 0
        combined_df = combined_df.join(df2, 'year_month', 'full_outer').select('*').fillna(0,subset=["sum(core_policy_invoked)","sum(core_policy_invoked)2","count","count2"])

        # sum the appropriate columns
        combined_df = combined_df.withColumn('total_core_policy_invoked',sum(combined_df[col] for col in ["sum(core_policy_invoked)","sum(core_policy_invoked)2"]))
        combined_df = combined_df.withColumn('total_rev_count',sum(combined_df[col] for col in ["count","count2"]))

        print("JOINED AND SUMMED STARTER AND DF2")
        combined_df.show(n=5,vertical=True)

        # need to reset combined_df to count and sum(core_policy_invoked) column names
        combined_df = combined_df.select(combined_df.year_month,combined_df.total_core_policy_invoked.alias('sum(core_policy_invoked)'), combined_df.total_rev_count.alias('count'))

        print("NEW STARTER")
        combined_df.show(n=5,vertical=True)

    # new column names for the returned df
    combined_df = combined_df.withColumnRenamed("count","total_rev_count")
    combined_df = combined_df.withColumnRenamed("sum(core_policy_invoked)","total_core_policy_invoked")

    return combined_df

def df_diff_get(input_df):
    # input_df should be regex_df in df_structurize
    regex_diff_df = input_df.orderBy("articleid")

    # 
    # year_month, {'ABC':0,'XYZ':3, ...} <-- a dictionary of counts

    # make an ordered list of year_month

    # iterate through the list and compare the dictionaries, get the diff

    # get the difference (diff_df) by month for each article

    # return a regex_diff_df that has the article_id, year_month, 

    return regex_diff_df.show()

if __name__ == "__main__":
    args = parse_args()
    conf = SparkConf().setAppName("wiki regex spark processing")
    spark = SparkSession.builder.getOrCreate()
    reader = spark.read

    #/gscratch/comdata/users/sohw/wikipi/wikiq_runs/output_samples/tsvSampleInputs
    print(args.input)
    print(args.lang)

    directory = "{}/{}wiki/*.tsv".format(args.input,args.lang)
    print(directory)

    files = glob.glob(directory)
    files = [os.path.abspath(p) for p in files]
    print(files)

    monthly_dfs = []

    #TODO SAMPLE AN ACTUAL FILE
    #sample = ["eswiki-20190901-pages-meta-history2-p257697p285980.tsv"]

    for tsv_f in files:
        print("Looking at: {}".format(tsv_f))
        regex_df = df_regex_make(tsv_f)
        # make it monthly
        monthly_df = df_monthly_make(regex_df)
        #monthly_df.show(n=5,vertical=True)

        print("\n======================================================================================================\n")

        monthly_dfs.append(monthly_df)

        # I was going to convert to pandas dataframe, but there doesn't seem to be much point here
        #monthly_pd = monthly_df.toPandas()
        #print(type(monthly_pd))
        #monthly_pd.head()

    #print(monthly_dfs)

    # take the list of monthly dfs and make the cumulative master df
    cumul_monthly = combine_dfs(monthly_dfs)
    cumul_monthly.show(n=10,vertical=True)
    print("--- %s seconds ---" % (time.time() - start_time))


    # convert the monthly master df to pandas df to tsv
    monthly_master_pdf = cumul_monthly.toPandas()

    out_filepath = "{}/{}{}.tsv".format(args.output_dir,args.output_filename,date.today())
    monthly_master_pdf.to_csv(args.output_dir,index=False,sep='\t')

