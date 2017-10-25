import argparse
import boto
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from lxml import etree
import json
import requests
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
parser.add_argument("--input", action="store", help="Path to Parquet file containing URLs.", default = "990_long/paths")
parser.add_argument("--output", action="store", help="Path in which to store result. Can be local or S3.", default="990_long/xml")
parser.add_argument("--timestamp", action="store_true", help="If true, append the timestamp to the output path.")
parser.add_argument("--partitions", type=int, action="store", help="Number of partitions to use for data retrieval.", default=500)

args = parser.parse_args()

if args.timestamp:
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
    suffix = "/%s" % timestamp
else:
    suffix = ""

outputPath = args.output + suffix

def getXml(url):
    r = requests.get(url)
    raw = r.text.encode("ascii", "ignore")
    return raw

udfGetXml = udf(getXml, StringType())

spark.read.parquet(args.input) \
        .repartition(args.partitions) \
        .withColumn("XML", udfGetXml("URL")) \
        .write.parquet(outputPath)

print "***Process complete."
