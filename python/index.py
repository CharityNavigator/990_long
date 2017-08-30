# Copyright 2017 Charity Navigator.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from pyspark import SparkContext
from pyspark.sql import SparkSession
import json
import requests

DEFAULT_YEARS = range(2011, 2018)

class Index:
    def __init__(self, limit, years = DEFAULT_YEARS):
        self.years = years
        self.limit = limit
        print "***IN INDEX*** limit = %i" % limit
        self.spark = SparkSession.builder.getOrCreate()

    def retrieveForYear(self, year):
        url  = "https://s3.amazonaws.com/irs-form-990/index_%i.json" % year
        r = requests.get(url)
        j = json.loads(r.text)
      
        # The index comes back as a single JSON key-value pair whose value is
        # a JSON array of length one. Inside _that_ is an array of filings.

        filings = j.values()[0]

        if self.limit != -1:
            sample = filings[0:self.limit]
        else:
            sample = filings

        return self.spark.createDataFrame(sample)

    def load(self):
        #SO 42540335
        dataframes = map(lambda r: self.retrieveForYear(r), self.years)
        union = reduce(lambda df1, df2: df1.unionAll(df2), dataframes)

        return union
