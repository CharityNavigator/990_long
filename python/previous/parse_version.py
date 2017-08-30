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

import sys
from pyspark import SparkContext
from lxml import etree
from schema.filing import RawXML
from schema.base import Credentials

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

session = makeSession()

def addVersions(records):
    session = makeSession()
    for row in records:
        curId = row[0]
        elem = session.query(RawXML).get(curId)
        root = etree.fromstring(elem.XML)
        elem.Version = root.attrib["returnVersion"]
        session.add(elem)
        session.commit()

sc = SparkContext()
records = session.query(RawXML.id, RawXML.Version)\
        .filter(RawXML.Version == None)

session.close()

sc.parallelize(records)\
        .foreachPartition(addVersions)
