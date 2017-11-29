import argparse
import boto
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import Row       # Yes, this is necessary despite above
from pyspark.sql.functions import *
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
parser.add_argument("--input", action="store", help="Path to file containing URLs.", default = "990_long/paths")
parser.add_argument("--output", action="store", help="Path in which to store result. Can be local or S3.", default="990_long/xml")
parser.add_argument("--timestamp", action="store_true", help="If true, append the timestamp to the output path.")
parser.add_argument("--partitions", type=int, action="store", help="Number of partitions to use for data retrieval.", default=500)
parser.add_argument("--format", action="store", help="Input format (parquet or csv).", default="parquet")

args = parser.parse_known_args()[0]

if args.timestamp:
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
    suffix = "/%s" % timestamp
else:
    suffix = ""

outputPath = args.output + suffix

def getXml(url):
    signal.alarm(2)
    try:
        r = requests.get(url)
    except:
        return None
    raw = r.text.encode("ascii", "ignore")
    return raw

def toS3(objId):
    return objId + "_public.xml"

def appendXml(row, conn):
    contents = row.asDict()
    s3name = toS3(contents["object_id"])
    xml = conn.get_key(s3name) \
            .get_contents_as_string() \
            .replace("\r", "")
    contents["xml"] = xml
    return contents

def getXmlForPartition(partition):
    conn = boto.connect_s3(host="s3.amazonaws.com") \
            .get_bucket("irs-form-990")

    ret = []

    for row in partition:
        contents = appendXml(row, conn)
        ret.append(contents)

    return ret

udfGetXml = udf(getXml, StringType())

if args.format == "parquet":
    input = spark.read.parquet(*args.input) 
elif args.format == "csv":
    input = spark.read.csv(*args.input, header=True) 
else:
    raise Exception("Unexpected input format \"%s\"" % args.input)

input.rdd \
        .repartition(args.partitions) \
        .mapPartitions(lambda p: getXmlForPartition(p)) \
        .map(lambda r: Row(**r)) \
        .toDF() \
        .write.parquet(outputPath)

print "***Process complete."
