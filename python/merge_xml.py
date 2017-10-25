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
parser.add_argument("--input", action="store", help="Path to Parquet file containing xpath-value pairs.")
parser.add_argument("--output", action="store", required=True, help="Path in which to store result. Can be local or S3.")
parser.add_argument("--timestamp", action="store_true", help="If true, append the timestamp to the output path.")
args = parser.parse_args()

if args.timestamp == None:
    suffix = ""
else:
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
    suffix = "/%s" % timestamp

outputPath = args.output + suffix

cc = spark.read.csv("concordance.csv", header=True)
cc.createOrReplaceTempView("concordance")

standardize = udf(lambda xpath : "/Return/" + xpath.strip(), StringType())
filings = spark.read.parquet(args.input) \
        .withColumn("xpath", standardize(col("xpath")))
filings.createOrReplaceTempView("filings")

query = """
    SELECT 
      f.*,
      c.variable_name as variable, 
      c.data_type as var_type,
      c.form as form,
      c.part as part,
      c.scope as scope,
      c.location_code as location
    FROM filings f
    LEFT JOIN concordance c
     ON f.xpath = c.xpath
    """

spark.sql(query) \
        .repartition("form", "part") \
        .write.partitionBy("form", "part") \
        .parquet(outputPath)
