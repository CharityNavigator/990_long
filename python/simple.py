from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from lxml import etree
import json
import requests
import re
import time
import datetime

timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
years = [2011, 2012]

def traverse(tree, node, elements):
    for child in node: 
        path = tree.getelementpath(child)
        path = re.sub("\{.*?\}", "", path)
        path = re.sub("\[.*?\]", "", path)
        value = child.text
        elements.append((path, value))
        traverse(tree, child, elements)

def doTraverse(tree, node):
    elements = []
    traverse(tree, node, elements)
    return elements

def retrieveForYear(year):
    url  = "https://s3.amazonaws.com/irs-form-990/index_%i.json" % year
    r = requests.get(url)
    j = json.loads(r.text)
  
    # The index comes back as a single JSON key-value pair whose value is
    # a JSON array of length one. Inside _that_ is an array of filings.

    filings = j.values()[0]
    sample = filings[0:100]
    return sample

def extractElements(root, template):
    tree = root.getroottree()
    elements = doTraverse(tree, root)
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

    return extractElements(root, template)

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

sc.parallelize(years) \
        .flatMap(lambda y : retrieveForYear(y)) \
        .flatMap(lambda f : parse(f)) \
        .map(lambda r : Row(**r)) \
        .toDF() \
        .write.format("com.databricks.spark.csv")\
        .option("header", "true")\
        .save(timestamp)
