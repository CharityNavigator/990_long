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
parser.add_argument("--output", action="store", help="Path in which to store result. Can be local or S3.", default = "s3a://cn-validatathon")
parser.add_argument("--timestamp", action="store_true", help="If true, append the timestamp to the output path.")
args = parser.parse_known_args()

if args.timestamp:
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
    suffix = "/%s" % timestamp
else:
    suffix = ""

outputPath = args.output + suffix

spark.read.parquet(args.input) \
        .repartition("form", "part") \
        .write.partitionBy("form", "part") \
        .parquet(outputPath)

print "*** Process complete."
