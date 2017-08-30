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
import boto
from boto.s3.key import Key
import time
from schema.base import Credentials
from schema.filing import Filing, RawXML
import sys

def key_to_str(bucket, key):
    start = time.time()
    ret = bucket.get_key(key) \
            .get_contents_as_string() \
            .replace("\r", "")
    end = time.time()
    #print "Retrieved %s in %0.2fs." % (key,end-start)
    return ret

cred = Credentials()

def makeSession():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    engineStr = cred.getEngineStr()
    engine = create_engine(engineStr)
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    return session

def loadXml(filings):
    session = makeSession()
    s3 = boto.connect_s3(host="s3.amazonaws.com")
    bucket = s3.get_bucket("irs-form-990")
    for filing in filings:
        if filing.URL == None:
            continue
        key_str = filing.URL.split("/")[-1]

        xml_str = key_to_str(bucket, key_str)
        e = RawXML(xml_str, filing)
        e.FormType = filing.FormType

        session.add(e)
        session.commit()
    session.close()


sc = SparkContext()
session = makeSession()

filings = session.query(Filing)\
        .filter(Filing.FormType == "990")\
        .filter(Filing.URL != None)\
        .filter(Filing.raw == None)

session.close()

sc.parallelize(filings)\
        .foreachPartition(loadXml)

session.close()
