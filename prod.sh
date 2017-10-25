spark-submit ./python/load_paths.py --prod
spark-submit ./python/load_xml.py
spark-submit ./python/parse_xml.py
spark-submit ./python/merge_xml.py
