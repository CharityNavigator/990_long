spark-submit ./python/load_paths.py
spark-submit --conf spark.speculation=true ./python/load_xml.py --partitions 20
spark-submit --conf spark.speculation=true ./python/parse_xml.py --partitions 20
#spark-submit ./python/merge_xml.py --output "s3://cn-validatathon/test" --timestamp
