import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from lxml import etree
import re
import time
import datetime
import sys

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

### SO 25407550
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

### Handle command line arguments

parser = argparse.ArgumentParser()
parser.add_argument("--input", action="store", help="Path to Parquet file containing xpath-value pairs.", default = "990_long/parsed")
parser.add_argument("--output", action="store", help="Path in which to store result. Can be local or S3.", default = "s3a://cn-validatathon-prep")
parser.add_argument("--timestamp", action="store_true", help="If true, append the timestamp to the output path.")
parser.add_argument("--partitions", type=int, action="store", help="Number of partitions to use for the join.", default=400)
args = parser.parse_args()

if args.timestamp:
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
    suffix = "/%s" % timestamp
else:
    suffix = ""

outputPath = args.output + suffix

cc = spark.read.csv("concordance.csv", header=True) \
        .select(col("xpath"),
                col("variable_name").alias("variable"),
                col("data_type").alias("var_type"),
                col("form"),
                col("part"),
                col("scope"),
                col("location_code").alias("location"))

standardize = udf(lambda xpath : "/Return/" + xpath.strip(), StringType())
df = spark.read.parquet(args.input) \
        .repartition(args.partitions) \
        .withColumn("xpath", standardize(col("xpath"))) \

df.join(cc, "xpath", "left") \
        .write \
        .parquet(outputPath)

print "*** Process complete."
