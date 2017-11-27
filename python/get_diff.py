import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from lxml import etree
import re
import time
import datetime
import sys
import signal

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

### SO 25407550
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

### Handle command line arguments

parser = argparse.ArgumentParser()
parser.add_argument("--paths", action="store", help="Path to Parquet file containing paths to 990 filings.")
parser.add_argument("--data", action="store", help="Path to Parquet file containing data to be diffed.")
parser.add_argument("--colname", action="store", help="Column name for object ID in data table.", default = "object_id")
parser.add_argument("--output", action="store", help="Path in which to store result. Can be local or S3.", default="990_long/parsed")
parser.add_argument("--timestamp", action="store_true", help="If true, append the timestamp to the output path.")
parser.add_argument("--partitions", type=int, action="store", help="Number of partitions to use for XML parsing.", default=500)
args = parser.parse_known_args()[0]

if args.timestamp:
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
    suffix = "/%s" % timestamp
else:
    suffix = ""

outputPath = args.output + suffix

paths = spark.read.parquet(args.paths)
data  = spark.read.parquet(args.data) \
           .select(col(args.colname).alias("object_id"))

missing = paths.join(data, "object_id", "leftanti")

missing.write.parquet(outputPath)
