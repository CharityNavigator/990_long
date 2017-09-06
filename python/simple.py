from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from lxml import etree
import json
import requests

years = [2011, 2012]

def retrieveForYear(year):
    url  = "https://s3.amazonaws.com/irs-form-990/index_%i.json" % year
    r = requests.get(url)
    j = json.loads(r.text)
  
    # The index comes back as a single JSON key-value pair whose value is
    # a JSON array of length one. Inside _that_ is an array of filings.

    filings = j.values()[0]
    sample = filings[0:10]
    return sample

def parse(filing):
    r = requests.get(filing["URL"])
    raw = r.text.encode("ascii", "ignore")
    
    root = etree.fromstring(raw)
    version = root.attrib["returnVersion"]

    c = filing.copy()
    c["version"] = version
    return c

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

sc.parallelize(years) \
        .flatMap(lambda y : retrieveForYear(y)) \
        .map(lambda f : parse(f)) \
        .map(lambda r : Row(**r)) \
        .toDF() \
        .show()
