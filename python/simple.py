import boto
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from lxml import etree
import json
import requests
import re
import time
import datetime
import sys

production = len(sys.argv) > 1 and sys.argv[1] == "--prod"

timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
years = range(2011, 2018)

def purify(text):
    if text == None:
        return None
    elif text.strip() == "":
        return None
    else:
        return text.strip()

def traverse(tree, node, elements, url):
    for child in node: 
        path = tree.getelementpath(child)
        path = re.sub("\{.*?\}", "", path)
        path = re.sub("\[.*?\]", "", path)
        value = purify(child.text)
        if len(child) > 0 and value != None:
            raise Exception("Path '%s' had %i children and text \"%s\" for URL: %s" % (path, len(child), child.text, url))
        if len(child) > 0:
            traverse(tree, child, elements, url)
        if value != None:
            elements.append((path, value))

def doTraverse(tree, node, url):
    elements = []
    traverse(tree, node, elements, url)
    return elements

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
        sample = filings[0:1000]
        return sample

def extractElements(root, template, url):
    tree = root.getroottree()
    elements = doTraverse(tree, root, url)
    r = []
    for (xpath, value) in elements:
        c = template.copy()
        c["xpath"] = xpath
        c["value"] = value
        r.append(c)
    return r

def parse(filing):
    r = requests.get(filing["URL"])
    raw = r.text.encode("ascii", "ignore")
    
    root = etree.XML(raw)

    version = root.attrib["returnVersion"]

    template = filing.copy()
    template["version"] = version

    return extractElements(root, template, filing["URL"])

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

#root
# |-- VAR_NAME: string (nullable = true)
# |-- FORM: string (nullable = true)
# |-- PART: string (nullable = true)
# |-- TABLE: string (nullable = true)
# |-- SCOPE: string (nullable = true)
# |-- PRODUCTION_RULE: string (nullable = true)
# |-- FULL_NAME: string (nullable = true)
# |-- DESCRIPTION: string (nullable = true)
# |-- LOCATION: string (nullable = true)
# |-- XPATH: string (nullable = true)
# |-- VERSION: string (nullable = true)
# |-- REQUIRED: string (nullable = true)
# |-- NOTES: string (nullable = true)
# |-- FLAG: string (nullable = true)
# |-- ANALYST: string (nullable = true)
# |-- LAST_UPDATED: string (nullable = true)
cc = spark.read.csv("concordance_long.csv", header=True)
cc.createOrReplaceTempView("concordance")


# root
# |-- DLN: string (nullable = true)
# |-- EIN: string (nullable = true)
# |-- FormType: string (nullable = true)
# |-- LastUpdated: string (nullable = true)
# |-- ObjectId: string (nullable = true)
# |-- OrganizationName: string (nullable = true)
# |-- SubmittedOn: string (nullable = true)
# |-- TaxPeriod: string (nullable = true)
# |-- URL: string (nullable = true)
# |-- value: string (nullable = true)
# |-- version: string (nullable = true)
# |-- xpath: string (nullable = true)
filings = sc.parallelize(years) \
        .flatMap(lambda y : retrieveForYear(y)) \
        .flatMap(lambda f : parse(f)) \
        .map(lambda r : Row(**r)) \
        .toDF()
standardize = udf(lambda xpath : "/Return/" + xpath.strip(), StringType())
filings = filings.withColumn("xpath", standardize(filings.xpath))
filings.createOrReplaceTempView("filings")

types = spark.read.csv("types.csv", header = True)
types.createOrReplaceTempView("types")
query = """
	SELECT 
          f.*, 
          c.VAR_NAME as variable, 
          t.Type as vartype,
          c.FORM as form,
          c.PART as part,
          c.SCOPE as scope,
          c.LOCATION as location,
          c.ANALYST as analyst
        FROM filings f
        LEFT JOIN concordance c
	 ON f.version = c.VERSION
	 AND f.xpath = c.XPATH
        LEFT JOIN types t
         ON f.xpath = t.Xpath
	"""

spark.sql(query) \
        .repartition("variable") \
        .write.partitionBy("variable") \
        .parquet("s3a://dbb-cn-transfer/990_long/%s" % timestamp)

print "Process complete."
