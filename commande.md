Lancement du feeder
```bash
docker exec -it spark-master /spark/bin/spark-submit   --master spark://spark-master:7077   /opt/pipeline/feeder.py   "file:///source/instagram.csv"   "jdbc:sqlite:/source/instagram_data.db"   "hdfs://namenode:9000/raw/instagram_data"
```
Lancement du preprocessor
```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077   /opt/pipeline/preprocessor.py   hdfs://namenode:9000/lakehouse/raw/instagram_data_raw
```
Verification a faire dans hive pour voir la database et les tables créer
```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
SHOW databases;
SHOW TABLES;
```
Lancement du datamarts
```bash
docker exec -it spark-master /spark/bin/spark-submit   --master spark://spark-master:7077   /opt/pipeline/datamarts.py   silver.instagram_data_users_enriched
```

Verification des tables gold créer dans postgres
```bash
docker exec -it postgres-instagram psql -U admin -d instagram_db
\dt
```
Lancer l'API
```bash
cd front/api
uvicorn api:app --reload
```