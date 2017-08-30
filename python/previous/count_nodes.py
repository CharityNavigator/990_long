# S3 Parquet handling based on this example:
# https://github.com/redapt/pyspark-s3-parquet-example/blob/master/pyspark-scripts/nations-parquet-sql-aws-emr.py

from lxml import etree
from operator import add
import re
import sys
import csv

def sterilize(txt):
    return txt.encode('ascii', 'ignore')
    #txt = re.sub("\\\\xef\\\\xbb\\\\xbf", "", txt)
    #txt = re.sub("\\\\n", "\n", txt)
    #return txt

# I think it's just a list, so we need to add a set
def traverse(tree, node, elements, version):
    for child in node.getchildren():
        path = tree.getelementpath(child)
        path = re.sub("\{.*?\}", "", path)
        path = re.sub("\[.*?\]", "", path)
        #key = ",".join([version, path])
        key="|&&|".join([version,path])
        elements.append(key)
        traverse(tree, child, elements, version)

def doTraverse(tree, node, version):
    elements = []
    traverse(tree, node, elements, version)
    return elements

def getElements(raw):
    version = sterilize(raw.Version)
    txt = sterilize(raw.XML)
    root = etree.XML(txt)
    tree = root.getroottree()
    return doTraverse(tree, root, version)

from pyspark import SparkContext
from pyspark import SQLContext
sc = SparkContext()

sql_context = SQLContext(sparkContext=sc)

# Loads parquet file located in AWS S3 into RDD Data Frame
xml_parquet = sql_context.read.parquet("s3://dbb-cn-transfer/xml")

# Stores the DataFrame into an "in-memory temporary table"
xml_parquet.registerTempTable("xml_table")

# Pull raw XML for each 990
raw_xml = sql_context.sql("SELECT Version, XML FROM xml_table")

counts = raw_xml.rdd \
                .flatMap(lambda x: getElements(x)) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(add) \
                .collect()

with open("counts.csv", "w") as out:
   writer = csv.writer(out)
   for key, count in counts:
       version, path = key.split("|&&|")
       row = [version, path, count]
       writer.writerow(row)
sc.stop()
