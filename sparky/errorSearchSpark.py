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
    parser.add_argument('-o', '--output-dir', help='Output directory', default='./CUMUL_MONTHLY_COUNTS', type=str)
    parser.add_argument('--output-format', help = "[csv, parquet] format to output",type=str)
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
    regexes_revid_df = regex_df.select(regex_df.revid,regex_df.articleid, regex_df.date_time,f.concat_ws(', ',*onlyRegexCols).alias("regexes"))
    # regexes_revid_df.show(vertical=True)


    #CRUNCHING FOR CUMULATIVE GRAPH - core revisions / total revisions
    # group by time --> month
    # regexes_revid_df.select(regex_df.revid,f.month(regex_df.date_time).alias('month'),f.year(regex_df.date_time).alias('year'),f.concat_ws(', ',*onlyRegexCols).alias('regexes')).show(vertical=True)
    #print(regexes_revid_df.columns)

    #CORE COLUMNS
    #ENGLISH:
    #SPANISH:	53_WIKIPEDIA:PUNTO_DE_VISTA_NEUTRAL, 69_WIKIPEDIA:WIKIPEDIA_NO_ES_UNA_FUENTE_PRIMARIA, 64_WIKIPEDIA:VERIFICABILIDAD
    #FRENCH: 	26_WIKIPÉDIA:NEUTRALITÉ_DE_POINT_DE_VUE, 19_WIKIPÉDIA:TRAVAUX_INÉDITS, 21_WIKIPÉDIA:VÉRIFIABILITÉ
    #GERMAN:
    #JAPANESE:	14_WIKIPEDIA:中立的な観点,16_WIKIPEDIA:独自研究は載せない, 15_WIKIPEDIA:検証可能性
    coreDFColumns = []
    if "53_WIKIPEDIA:PUNTO_DE_VISTA_NEUTRAL" in onlyRegexCols:
        coreDFColumn = [c for c in onlyRegexCols if (c[:2]==str(53) or c[:2]==str(69) or c[:2]==str(64))]
    elif "26_WIKIPÉDIA:NEUTRALITÉ_DE_POINT_DE_VUE" in onlyRegexCols:
        coreDFColumn = [c for c in onlyRegexCols if (c[:2]==str(26) or c[:2]==str(19) or c[:2]==str(21))]
    else:
        coreDFColumn = [c for c in onlyRegexCols if (c[:2]==str(14) or c[:2]==str(15) or c[:2]==str(16))]

    #print(coreDFColumn)

    monthly_regex_df = regex_df.select(regex_df.revid, f.concat_ws('_',f.year(regex_df.date_time),f.month(regex_df.date_time)).alias('year_month'),f.concat_ws(', ',*coreDFColumn).alias('core_regex'))
    monthly_regex_df = monthly_regex_df.na.replace('',None)
    #monthly_regex_df.show()
    monthly_regex_df = monthly_regex_df.select(*monthly_regex_df,f.when(monthly_regex_df.core_regex.isNotNull(),1).otherwise(0).alias('core_policy_invoked'))

    #monthly_regex_df.show()

    monthly_core_count_df = monthly_regex_df.groupBy('year_month').sum('core_policy_invoked')
    monthly_revn_count_df = monthly_regex_df.groupBy('year_month').count()

    monthly_joined_df = monthly_revn_count_df.join(monthly_core_count_df, on=['year_month'],how='left')

    # MONTHLY CUMULATIVE COUNTS
    #monthly_joined_df.orderBy(monthly_joined_df.year_month).show()
    
    #print(files[0][55:])
    #infilename = files[0][55:]

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)
    if args.output_format == "csv" or args.output_format == "tsv":
        monthly_joined_df.coalesce(1).write.csv(args.output_dir, sep='\t', mode='append',header=True)


    #TODO NEW DATAFRAME regex_diff_df that gets the regex diff for revid and revid_prior
    #regex_diff_df = regexes_revid_df.select(*regexes_revid_df.columns)
    #regex_diff_df = regexes_revid_df.na.replace('',None)
    #the_window = Window.partitionBy("articleid").orderBy("date_time")
    #regex_diff_df = regex_diff_df.withColumn("previous_regexes", f.lag(regex_diff_df.regexes).over(the_window))
    #regex_diff_df = regex_diff_df.withColumn("regexes_len", f.when(regex_diff_df.regexes.isNotNull(), f.length(regex_diff_df.regexes)).otherwise(0))
    #regex_diff_df = regex_diff_df.withColumn("prev_regexes_len", f.when(regex_diff_df.previous_regexes.isNotNull(), f.length(regex_diff_df.previous_regexes)).otherwise(0))
    #regex_diff_df = regex_diff_df.withColumn("regexes_new", f.when(regex_diff_df.regexes.contains(regex_diff_df.previous_regexes) ,1).otherwise(0))

    #... f.when( f.length(regex_diff_df.regexes) > f.length(regex_diff_df.previous_regexes),1 ).otherwise(0))

    #regex_diff_df.filter(regex_diff_df.regexes_len > 0).show()


    # CHECKING REGEX COLUMNS --- USUALLY LEAVE THIS LOOP COMMENTED OUT
    #for c in onlyRegexCols:
    #    print(c)
    #    print(type(c))


    """
    #ERROR DETECTION
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
    """    
    print("RUNNING TIME: {}".format(time.time() - start_time))

