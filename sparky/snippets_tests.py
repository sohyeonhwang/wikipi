from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql import types
import sys



df = spark.read.csv("./eswiki_sample1.tsv",sep="\t",header="True")

df.show()
