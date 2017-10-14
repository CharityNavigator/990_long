# 990_long
Generates a long-form version of every field in the IRS 990 eFile dataset based on the Nonprofit Open Data Collective (NOPDC) "Datathon" concordance. The concordance is the work of many separate groups that came together to make sense of the IRS Form 990 eFile dataset. An upcoming NOPDC website will have more information.

# Bootstrap
This code can be run in test mode or production mode. In test mode, it samples 1000 eFiles from each year's 990s. For production mode, it collects all of them. This code is a public draft, and the production process is still a little raw. As of this writing (2017-10-14), you still need to do two things manually:

* Make sure you have the latest [concordance file](https://github.com/Nonprofit-Open-Data-Collective/irs-efile-master-concordance-file)
* Hard-code the S3 location where you want the output to be saved

It is most convenient to run this code from an IAM account that has access to EMR, EC2, S3, and Amazon Athena all together. 

To run the code:

Create an EMR cluster configured for Spark applications. As a benchmark, a 3-machine cluster consisting of `m4.xlarge` machines can run the test data in about 5 minutes. The production data is around 350 times large as of 2017-10-14. So a 10-node cluster of `c4.8xlarge` should be enough to get it all done in an hour. (There are other variables--this is a ballpark.)

Next, SSH into the master node and run the following code:

```
sudo yum -y install git
git clone https://github.com/CharityNavigator/990_long
cd 990_long/data
hadoop fs -put types.csv
```

If I'm going to use this cluster to inspect the results of the job, I find it convenient to also install `htop` and `tmux`.

If you have reason to believe that the concordance in this repository is not the latest version, also clone [the concordance repo](https://github.com/Nonprofit-Open-Data-Collective/irs-efile-master-concordance-file) and copy it over `990_long/data/concordance.csv`.

Now run

```
hadoop fs -put concordance.csv
cd ../python
vim simple.py
```

Go to the bottom of `simple.py` and edit the S3 location where you want the data to be stored. Save and exit.

For test, type

```
nohup sh -c "spark-submit simple.py > out 2> err" &
```

For production, type
```
nohup sh -c "spark-submit simple.py > out 2> err" &
```

Since you ran the job using `nohup`, it will keep running even if you log out of your machine.

Once the application is finished running, you are ready to use the output. For the Validatathon, we will be intereacting with it using Amazon Athena. Go to the [Athena Console] and run the following query, changing `name_of_table` and `s3://my-bucket/path-to-parquet/` to their appropriate values. 

Note that Athena will read *all* of the little files (partitions) that Spark created, so you give it the same path you gave Spark, except this time you'll use `s3://` instead of `s3a://`. 

```
CREATE EXTERNAL TABLE `name_of_table`(
  `dln` string, 
  `ein` string, 
  `form_type` string, 
  `last_updated` string, 
  `object_id` string, 
  `organization_name` string, 
  `submitted_on` string, 
  `tax_period` string, 
  `url` string, 
  `value` string, 
  `version` string, 
  `xpath` string, 
  `variable` string, 
  `vartype` string, 
  `form` string, 
  `part` string, 
  `scope` string, 
  `location` string, 
  `analyst` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://my-bucket/path-to-parquet/'
TBLPROPERTIES (
  'has_encrypted_data'='false', 
  'transient_lastDdlTime'='1507989450')
```

You're almost there. Follow [these steps] to access the data in RStudio. For the validatathon, we are creating one EC2 instance configured in this way, and then creating clones of it for each participant.

## License

*Copyright (c) 2017 Charity Navigator.*

*Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:*

*The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.*

*THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.*
