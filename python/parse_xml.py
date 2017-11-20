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
parser.add_argument("--input", action="store", help="Path to Parquet file containing raw XML.", default="990_long/xml")
parser.add_argument("--output", action="store", help="Path in which to store result. Can be local or S3.", default="990_long/parsed")
parser.add_argument("--timestamp", action="store_true", help="If true, append the timestamp to the output path.")
parser.add_argument("--partitions", type=int, action="store", help="Number of partitions to use for XML parsing.", default=500)
parser.add_argument("--timeout", type=int, action="store", help="Number of seconds to spend parsing a single 990 eFile before it is skipped.", default=3)
args = parser.parse_known_args()

if args.timestamp:
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
    suffix = "/%s" % timestamp
else:
    suffix = ""

outputPath = args.output + suffix

def purify(text):
    if text == None:
        return None
    elif text.strip() == "":
        return None
    else:
        return text.strip()

def traverse(tree, node, elements):
    for child in node: 
        path = tree.getelementpath(child)
        path = re.sub("\{.*?\}", "", path)
        path = re.sub("\[.*?\]", "", path)
        value = purify(child.text)
        if len(child) > 0:
            traverse(tree, child, elements)
        if value != None:
            elements.append((path, value))

def doTraverse(tree, node):
    elements = []
    traverse(tree, node, elements)
    return elements

def extractElements(root, version):
    tree = root.getroottree()
    elements = doTraverse(tree, root)
    r = []
    for (xpath, value) in elements:
        c = [version, xpath, value]
        r.append(c)
    return r

def handler(signum, frame):
    raise Exception()

def parse(raw):
    signal.signal(signal.SIGALRM, handler)
    try:
        signal.alarm(args.timeout)
        ascii = raw.encode("ascii", "ignore")
        root = etree.XML(ascii)
        version = root.attrib["returnVersion"]
        ret = extractElements(root, version)
        signal.alarm(0)
        return ret
    except Exception:
        return []

schema = ArrayType(
        StructType([
            StructField("version", StringType(), False),
            StructField("xpath", StringType(), False),
            StructField("value", StringType(), False)
        ]))


udfParse = udf(parse, schema)

spark.read.parquet(args.input) \
        .repartition(args.partitions) \
        .withColumn("elementArr", udfParse("XML")) \
        .select(col("DLN").alias("dln"), 
                col("EIN").alias("ein"), 
                col("ObjectId").alias("object_id"),
                col("OrganizationName").alias("org_name"),
                col("SubmittedOn").alias("submitted_on"),
                col("TaxPeriod").alias("period"),
                col("URL").alias("url"),
                explode(col("elementArr")).alias("element")) \
        .withColumn("version", col("element.version")) \
        .withColumn("xpath", col("element.xpath")) \
        .withColumn("value", col("element.value")) \
        .drop("element") \
        .write.parquet(outputPath)

print "***Process complete."
