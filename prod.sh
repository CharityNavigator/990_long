spark-submit ./python/load_paths.py --prod
spark-submit --conf spark.speculation=true ./python/load_xml.py
spark-submit --conf spark.speculation=true ./python/parse_xml.py
spark-submit ./python/merge_xml.py --timestamp
