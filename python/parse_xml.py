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
parser.add_argument("--input", action="store", help="Path to Parquet file containing raw XML.")
parser.add_argument("--output", action="store", required=True, help="Path in which to store result. Can be local or S3.")
parser.add_argument("--timestamp", action="store_true", help="If true, append the timestamp to the output path.")
args = parser.parse_args()

if args.timestamp == None:
    suffix = ""
else:
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
    suffix = "/%s" % timestamp

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
            traverse(tree, child, elements, url)
        if value != None:
            elements.append((path, value))

def doTraverse(tree, node):
    elements = []
    traverse(tree, node, elements, url)
    return elements

def extractElements(root, version, url):
    tree = root.getroottree()
    elements = doTraverse(tree, root, url)
    r = []
    for (xpath, value) in elements:
        c = [version, xpath, value]
        r.append(c)
    return r

def parse(raw):
    root = etree.XML(raw)
    version = root.attrib["returnVersion"]
    return extractElements(root, version)

schema = ArrayType(
        StructType([
            StructField("version", StringType(), False),
            StructField("xpath", StringType(), False),
            StructField("value", StringType(), False)
        ]))


udfParse = udf(parse, schema)

spark.read.parquet(args.input) \
        .repartition(200) \
        .withColumn("elementArr", udfParse("XML")) \
        .select(col("DLN").alias("dln"), 
                col("EIN").alias("ein"), 
                col("ObjectId").alias("object_id"),
                col("OrganizationName").alias("org_name"),
                col("SubmittedOn").alias("submitted_on"),
                col("TaxPeriod").alias("period"),
                col("URL").alias("url"),
                explode(col("elementArr").alias("element"))) \
        .withColumn("version", col("element.version")) \
        .withColumn("xpath", col("element.xpath")) \
        .withColumn("value", col("element.value")) \
        .write.parquet(outputPath)

print "***Process complete."
