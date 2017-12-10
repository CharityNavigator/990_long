#!/bin/bash
cd ~
echo "***Installing packages."
sudo yum -y install htop tmux git
echo "*** Cloning 990_long."
git clone https://github.com/CharityNavigator/990_long 
echo "*** Cloning concordance."
git clone https://github.com/Nonprofit-Open-Data-Collective/irs-efile-master-concordance-file.git 
echo "*** Copying concordance to 990_long."
cp irs-efile-master-concordance-file/efiler_master_concordance.csv 990_long/data/concordance.csv
echo "*** Putting concordance into HDFS."
hadoop fs -put 990_long/data/concordance.csv 
echo "*** Changing directory to 990_long"
cd 990_long || exit 1
echo "*** Running load_paths"
spark-submit --conf spark.speculation=true python/load_paths.py --output "$1/paths"  "${@:2}" || exit 1
echo "*** Running load_xml"
spark-submit --conf spark.speculation=true python/load_xml.py --input "$1/paths" --output "$1/xml" "${@:2}" || exit 1
echo "*** Running parse_xml"
spark-submit --conf spark.speculation=true python/parse_xml.py --input "$1/xml" --output "$1/parsed" "${@:2}" || exit 1
echo "*** Running merge_xml"
spark-submit --conf spark.speculation=true python/merge_xml.py --input "$1/parsed" --output "$1/merged" "${@:2}" || exit 1
echo "*** Running split_csv"
spark-submit --conf spark.speculation=true python/split_csv.py --input "$1/merged" --output "$1/csv" "${@:2}" || exit 1
