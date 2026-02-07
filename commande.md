docker exec -it spark-master /spark/bin/spark-submit /opt/pipeline/feeder.py

docker exec -it spark-master /spark/bin/spark-submit /opt/pipeline/verif.py

docker exec -it spark-master /spark/bin/spark-submit   --master spark://spark-master:7077   /opt/pipeline/feeder.py   "file:///source/instagram.csv"   "jdbc:sqlite:/source/instagram_data.db"   "hdfs://namenode:9000/raw/instagram_data"

docker exec -it spark-master /spark/bin/spark-submit   --master spark://spark-master:7077   /opt/pipeline/preprocessor_HACI.py   "hdfs://namenode:9000/lakehouse/raw/instagram_data_raw"

docker exec -it spark-master /spark/bin/spark-submit   --master spark://spark-master:7077   /opt/pipeline/datamarts.py   "hdfs://namenode:9000/lakehouse/raw/instagram_data_raw"

psql -U admin -d instagram_db
