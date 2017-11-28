import argparse
import boto
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
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
parser.add_argument("--prod", action="store_true", help="If not set, will only retrive 1000 filings per year")
parser.add_argument("--test-count", type=int, action="store", help="If running in test mode, number of 990s to pull per year.", default = 1000)
parser.add_argument("--output", action="store", help="Path in which to store result. Can be local or S3.", default = "990_long/paths")
parser.add_argument("--timestamp", action="store_true", help="If true, append the timestamp to the output path.")
parser.add_argument("--earliest-year", type=int, action="store", default=2011, help="First year to include in data.")
args = parser.parse_known_args()[0]

production = args.prod

if args.prod:
    print "Production mode is ON!"
else:
    print "Production mode is OFF. For test mode, I will retrieve %i records per year, starting with the year %i." % (args.test_count, args.earliest_year)

if args.timestamp:
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
    suffix = "/%s" % timestamp
else:
    suffix = ""

outputPath = args.output + suffix

def retrieveForYear(year):
    r = boto.connect_s3(host="s3.amazonaws.com") \
            .get_bucket("irs-form-990") \
            .get_key("index_%i.json" % year) \
            .get_contents_as_string() \
            .replace("\r", "")
    j = json.loads(r)
  
    # The index comes back as a single JSON key-value pair whose value is
    # a JSON array of length one. Inside _that_ is an array of filings.

    filings = j.values()[0]

    if production:
        return filings
    else:
        sample = filings[0:args.test_count]
        return sample

def getYears(first_year):
    year = first_year
    failed = False
    years = []

    bucket = boto.connect_s3(host="s3.amazonaws.com") \
            .get_bucket("irs-form-990")

    while not failed:
        failed = not bucket.get_key("index_%i.json" % year)
        if not failed:
            years.append(year)
        year += 1

    return years

years = getYears(args.earliest_year)

sc.parallelize(years) \
        .flatMap(lambda y : retrieveForYear(y)) \
        .map(lambda r : Row(**r)) \
        .toDF() \
        .select(col("EIN").alias("ein"),
                col("TaxPeriod").alias("period"),
                col("DLN").alias("dln"),
                col("FormType").alias("form"),
                col("URL").alias("url"),
                col("OrganizationName").alias("org_name"),
                col("SubmittedOn").alias("submitted_on"),
                col("ObjectId").alias("object_id"),
                col("LastUpdated").alias("updated")) \
        .write.parquet(outputPath)

print "***Process complete."
