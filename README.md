# `990_long`

Generates a long-form version of every field in the IRS 990 eFile dataset based on the Nonprofit Open Data Collective (NOPDC) "Datathon" concordance. The concordance is the work of many separate groups that came together to make sense of the IRS Form 990 eFile dataset. **The concordance is very much still a draft**; at this time, the output of `990_long` should be treated as preview data. You can view the concordance at its own [Github repo](https://github.com/Nonprofit-Open-Data-Collective/irs-efile-master-concordance-file).

The output fdata is a huge list of key-value pairs (KVPs), plus metadata. These key-value pairs are stripped of any *structure*. As an example, take compensation data from Part VII(a) of the core 990. For each person listed, there is a name, hours worked, compensation, and a number of checkboxes. The output of this script will give you each of these things without any way to link them to the others. It will, however, provide some information about the filing (and organization) that it came from, meaning that it can be very useful for analysis.

Again, though, these data are riddled with mistakes. If you want to clean it up, please consider contributing to the [concordance](https://github.com/Nonprofit-Open-Data-Collective/irs-efile-master-concordance-file).

# Running the code

This repository is designed to be used with Amazon Web Services. The code runs on Spark over Elastic Map Reduce (EMR), producing assets that are stored in Amazon Simple Storage Service (S3). If you decide to use the code in another environment, please create documentation and share it!

## Setting up the environment

These instructions assume some familiarity with Amazon Web Services. They also assume that you have an account. 

### Step 1: Create an S3 bucket

You will need to store your output somewhere. In parquet format, the master file is over 56gb. **In .csv format, the output is over 250gb.** Be aware of the associated costs before you start. Your bucket should be accessible to your EMR instance. There is a test mode that produces a vastly smaller version of the output.

### Step 2: Choose your EMR cluster

I assume that you will using your cluster exclusively for processing these data. For that reason, we can optimize the cluster for single-purpose use by turning on the EMR-specific `maximizeResourceAllocation` flag. This configures Spark to give as much resources (RAM and cores) to each worker as possible. This also means that your cluster will become unstable if you try to do anything else at the same time--including use `pyspark`. If you want to examine the output of a step while running the next step, you should store your output directly on S3 rather than on local storage.
 
As far as I can tell, the master node doesn't get used for much at any point in this process. However, I'm not a Spark expert, so I hedged my bets by not making the master node too small. If you have any insight into the question of head node size, please provide create a ticket so we can discuss.
 
#### If you are doing a test run (7000 filings)

For test purposes, a small cluster is fine. I found that two `m4.xlarge` workers plus an `m4.xlarge` master node was enough power to run and examine the test data in interactive time. At current on-demand rates, this puts you in the vicinity of $1 per hour. As I mentioned above, you may be able to get away with a smaller head node.

#### If you are doing a production run (>2M filings)

For production purposes, you need more firepower. When I last ran the code, I used an `i3.xlarge` head node, a single `i3.xlarge` core node, and then 20 `r4.xlarge` task nodes purchased on spot. Why? The head and core nodes must be purchased on-demand so that they don't get shut down mid-run. But your spot instances can come and go. Both `r4` and `i3` have lots of memory, which is especially helpful for the XML processing step. The `i3` type also has lots of built-in storage, which lets us store data from in-between steps locally (which is fast) until we move the final product to the cloud. The task nodes, which are transient, don't need any storage, so `r4` is fine.

At current EMR pricing, the `i3` nodes are about $0.40 per hour each. Using spot instances for the `r4` task nodes, we can get those for about $0.13 apiece. All told, this cluster costs about $3.50 per hour to run in compute resources. I don't think there are other costs during runtime, but then again, we haven't been too worried about it.

On this cluster, the approximate running time for each step was as follows:

  * `load_paths.py`: 2 minutes to read the 990 index files (in JSON format) from S3 into local tables (in parquet format)
  * `load_xml.py`: 30 minutes to read in all the XML files and append the raw XML to the local index tables. (See note 1)
  * `parse_xml.py`: 14 minutes to parse all of the XML data into a key-value representation, skipping any that take more than three seconds to process. (See note 2)
  * `merge_xml.py`: 9 minutes to merge concordance data, including variable names and metadata, into the key-value mapping.
  * `split_csv.py`: 15 minutes to split the merged data into .csv files, if desired.
 
In theory, therefore, this entire thing could be done for less than $10. In practice, often times pieces fail and you have to go in there and re-run them. Add in periods of inattention, and the whole thing might take you a few hours of operating time.

Note 1: This process is much slower than you'd expect for the volume of data. The reason is that the IRS put all two million XML files into a single directory. This causes the `aws s3 ls` and `aws s3 sync` commands to become uselessly slow. We thus have to go in and request each one by name. 

Note 2: Most are finished in a tiny fraction of a second, so I assume that the slow ones are the result of hiccups or errors. That said, I have not yet had a chance to go through and look at which ones were skipped, which can be accomplished by finding the `object_id` values in the index that fail to appear in the finished product. Hopefully, hitting these a second time, with fewer files per node, will prove sufficient to handle them all. If there is a bug that's causing them to run slowly, however, a tweak in the code may be required.

### Step 3: Turn on your EMR cluster

Whatever size cluster you're running, you'll now need to turn it on. You'll want to specify the `MaximizeResourceAllocation` option, which must be specified in advance. This saves you a lot of math (and typing) by figuring out how much memory and how many cores can go to each executor, given the cluster you chose. (Quick refresher: your data will be broken into chunks, called "partitions." Processing a partition is a task. Each task gets performed by an executor. You have a certain number of executors per node. The nodes are what you're choosing and buying.) 

While we're at it, we'll set a second parameter, `spark.shuffle.io.maxRetries`. Your spot instances will get shut down whenever someone shows up who's willing to pay a higher price. This includes everyone running on-demand, so expect it to happen. When that happens, whatever was running on it will fail. By default, when that happens three times to any one task, the whole run is killed, and you have to start the step you're running over. So we crank up the number of retries we're allowed. You can set this one on the fly, but since we're already here, let's add it now.

So to set these parameters, you'll need to use the `Advanced mode` screen when creating your EMR cluster.  Start with the `Create cluster` button, then choose `Go to advanced options`. Check off Spark and Ganglia. Under `Edit software settings`, put in the following:

```
classification=spark-defaults,properties=[maximizeResourceAllocation=true,spark.shuffle.io.maxRetries=9]
```

Hit "next," and then put in the computers you chose. If you're running in production, and you did not choose `i3` computers, you'll definitely want to add some extra EBS storage to your master and core nodes. (Not the root device EBS volume size, but the one underneath each instance type in the list of nodes, with a pencil symbol next to it. The default is usually `32gb` or `none`.) Be sure to choose "spot" for your task nodes, and "on-demand" for your master and core nodes. You can view spot price history from the EC2 console.

Hit "next" again. Choose a name that will help you identify this cluster six months from now. Hit "next" one more time. Do NOT proceed without an EC2 key pair: choose one or create one. You are now ready to create your cluster. It will take about 15 minutes for it to finish bootstrapping.

### Step 4: Walk through the steps

Find your master node's public IP address by clicking on your cluster, choosing the hardware tab, and clicking on its ID. SSH into it as user `hadoop` using your EC2 private key (`.pem` file). At the command line, that's

```
ssh -i my-key.pem hadoop@111.222.333.444
```

where `my-key.pem` is your private key and `111.222.333.444` is your master node's IP address. Once you're in, it's time to set things up. (If you do this frequently, you might want to create a shell script and store it on S3 to use as a bootstrap step.) Run the following:

```
sudo yum -y install htop tmux git
git clone https://github.com/CharityNavigator/990_long
git clone https://github.com/Nonprofit-Open-Data-Collective/irs-efile-master-concordance-file.git
cp irs-efile-master-concordance-file/efiler_master_concordance.csv 990_long/data/concordance.csv
hadoop fs -put 990_long/data/concordance.csv
cd 990_long
```

Now you're ready to start running the code. There are shell scripts sitting in the `990_long` directory, but I wouldn't bother using them. The steps can fail for one reason or another, and you'll just wind up trying to figure out what happened. (That's also why I run them as separate steps instead of all at once--so that when things fail, you're not starting over.)

#### Loading indices into a local parquet file

The first step is to pull the 990 indices that are hosted in AWS and load them into a local parquet file, ready to be used in subsequent steps. We'll run it in the background, using `nohup` so that it keeps running even if we get disconnected. Then we'll monitor the logs using `tail -f` to make sure it doesn't blow up. We'll take the same approach for all other steps.

```
nohup sh -c "spark-submit python/load_paths.py [--prod]" > load_paths.out 2> load_paths.err &
tail -f load_paths.err
```
  
You can exit `tail` by pressing `ctrl+c`. 

If you include `--prod`, it will pull index info for all the 990s. If you don't it will only pull the first thousand for each year. (Each subsequent step uses the output of the one before, so you only need to specify this at the outset.) There are other options as well; you can specify output path, first year, and whether or not to append a timestamp to the output path location. If you don't specify anything, the data will go into `990_long/paths`, which also happens to be the default input to the next step. (Convenient!)

#### Retrieving raw XML

Now that we know what 990s we want, we have to get them. As mentioned above, we retrieve them one at a time from S3 using [boto](http://boto.cloudhackers.com/en/latest/). As with the preceding step, we'll run it in the background using `nohup` and then monitor the progress with `tail`. Assuming you used the default output location for `load_paths.py` and you're happy to do the same for `load_xml.py`:

```
nohup sh -c "spark-submit python/load_xml.py" > load_xml.out 2> load_xml.err &
tail -f load_xml.err
```

This step used to be a bit finnicky, but I think it should be working pretty well now. If you're not running in production mode, you'll probably want to drop the number of partitions considerably by adding, e.g., `--partitions 20` to the `spark-submit` command. Other than that, if you run into any trouble, please [create a ticket](https://github.com/CharityNavigator/990_long/issues/new), and attach your `load_xml.err` and `load_xml.out` along with a description of your EMR cluster and the preceding steps.

#### Parsing the raw XML

Now we do the heavy lifting. We need to crawl through all of those XML documents and turn them into key-value pairs. This process definitely gets bogged down, for reasons I have not yet had a chance to diagnose. It does eventually finish if you wait long enough. Time is money on EMR, though, so I added a `--timeout` argument. By default, if it takes more than three seconds to parse a 990, the script gives up. Most 990s process within a tiny fraction of a second. Without the timeout, the run can take many hours, but with it, it's pretty quick. The commands:

```
nohup sh -c "spark-submit python/parse_xml.py" > parse_xml.out 2> load_xml.err &
tail -f load_xml.err
```

#### Merging the key-value pairs with the concordance

Everything we've done so far can be done without the concordance. Now it's time to get the variable-level insights that can only be identified by human review. This step is not as resource-intensive as the parsing step, because it's the kind of thing for which the Hadoop universe is optimized. I haven't had any trouble with this step. Note that you will need to specify where you want this output to go, because the default location is particular to the reason for which this repo was created: the IRS Form 990 Validatathon event, which took place at the Aspen Institute on Nov 1-2, 2017. 

```
nohup sh -c "spark-submit python/merge_xml.py --output \"my/destination\"" > merge_xml.out 2> merge_xml.err &
tail -f merge_xml.err
```

As far as what to put in place of `my/destination`, see the note on specifying locations below.

#### (optional) create .csv files

Parquet files are nice and all, but you probably want to look at the data without using Spark. There are two straightforward options: Amazon Athena and .csv files. Creating .csv files is as simple as running one more script:

```
nohup sh -c "spark-submit python/split_csv.py --input \"my/origin\" --output \"my/destination\"" > split_csv.out 2> split_csv.err &
tail -f split_csv.err
```

Amazon Athena requires a bit more explanation, discussed in step 7.

#### A note on specifying locations

At each step, you may want to specify a particular place to find input data or put output data. If you use the `--input` and `--output` paths, you'll need to quote them so that the punctuation doesn't mess things up. And since the `nohup sh -c` call is already quoted, you must escape those quotes. Here's an example of a correctly constructed command.

```
nohup sh -c "spark-submit python/load_paths.py --output \"s3a://my-bucket/location\"" > load_paths.out 2> load_paths.err &
```

Notice the `s3a://`. This is an idiosyncracy of EMR, but you'll want to use `s3a://` rather than `s3://` for maximum performance. If you want to keep your data local to your cluster, you can just specify it as:

```
nohup sh -c "spark-submit python/load_paths.py --output \"foo/bar\"" > load_paths.out 2> load_paths.err &
```

This does **not** store your data into local storage on the master node; it puts it into HDFS, which is distributed over your master and core nodes, and which is accessed using a separate set of commands like `hadoop fs` and `s3-dist-cp`. It is more local than S3, and less local than your master machine's hard drive.

### Step 5: Transfer the data to S3 (if needed)

If you put the merged data directly onto S3, you may be satisfied at this point. If, however, you want to move any data from HDFS to S3, you'll need to jump through one more hoop. You'll be using a command called `s3-dist-cp`, but the documentation leaves much to be desired. The syntax is simple enough:

```
s3-dist-cp --src hdfs://server:port/my/location --dest s3://my-bucket/path
```

The trouble is knowing what to put for the `server` and `port` in that `hdfs` URL. The `server` is the private DNS for your head node. This can be found by going to the EMR console, clicking on your cluster, going to the "Hardware" tab, and clicking on your master node's ID. You want the *private* DNS name. (The public one won't even work with the default security settings, and if it did, you'd be paying some hefty transfer charges.) The port number is `8020`, because of course it is. 

Actually, that's not all the trouble. Even once you got all that figured out, you'd still have to notice that the directory structure that you see when you type `hadoop fs -ls` is not the root--it's relative to `/user/hadoop`. So your command ends up looking like this:

```
s3-dist-cp --src hdfs://ip-111-222-333-444.ec2.internal:8020/user/hadoop/990_long/merged --dest s3://my-bucket/destination
```

### Step 6: Shut down your cluster!

This cluster will run up a BIG bill if you do not shut it down, right now. Go to the EMR console and choose "terminate." If you enabled termination protection, you will have to disable it first.

### Step 7: Create an Athena table or .csv files

Athena is based on Apache Presto. It lets you treat a data file like a database, without actually running a database. For rarely used data, it's incredibly cheap, even when the data are really big. To set up an Athena table, go to the Athena console on AWS. Then run the following query:

```
CREATE EXTERNAL TABLE `my_table_name`(
  `xpath` string, 
  `dln` string, 
  `ein` string, 
  `object_id` string, 
  `org_name` string, 
  `submitted_on` string, 
  `period` string, 
  `url` string, 
  `version` string, 
  `value` string, 
  `variable` string, 
  `var_type` string, 
  `form` string, 
  `part` string, 
  `scope` string, 
  `location` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://path-to-my-data'
```


You can now use this table, for example, to analyze the data in RStudio. To do so, follow [these steps](https://aws.amazon.com/blogs/big-data/running-r-on-amazon-athena/).

## License

*Copyright (c) 2017 Charity Navigator.*

*Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:*

*The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.*

*THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.*
