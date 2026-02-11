# -*- coding: utf-8 -*-
import sys, os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# _____ CONFIGURATION DES LOGS _____
# Le sujet impose l'export des logs dans un fichier .log
log_dir = "/opt/pipeline/logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_filename = "/opt/pipeline/logs/feeder_logs.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_filename,
    filemode='a'
)
logger = logging.getLogger("FeederApp_RAW")


def main():
    # Validation des arguments
    if len(sys.argv) < 3:
        logger.error("Arguments manquants. Usage: feeder.py <path_csv> <jdbc_url>")
        sys.exit(1)

    csv_input_path = sys.argv[1]
    jdbc_url = sys.argv[2]

    spark = SparkSession.builder \
        .appName("Instagram_Bronze_JDBC_Feeder") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        logger.info("Debut de l'ingestion RAW.")

        # _____LECTURE CSV_____
        logger.info("Lecture du CSV: {}".format(csv_input_path))
        df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_input_path)

        # _____LECTURE JDBC (SQLite)_____
        logger.info("Lecture JDBC SQLite: {}".format(jdbc_url))
        df_db = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "Instagramme_Usage_Logs") \
            .option("driver", "org.sqlite.JDBC") \
            .load()

        # _____JOINTURE (Pour la couche RAW)_____
        df_joined = df_csv.join(df_db, on="user_id", how="left")

        # _____PARTITIONNEMENT DYNAMIQUE_____
        now = datetime.now()
        df_raw = df_joined \
            .withColumn("year", F.lit(now.year)) \
            .withColumn("month", F.lit("{:02d}".format(now.month))) \
            .withColumn("day", F.lit("{:02d}".format(now.day)))

        # _____ECRITURE TABLE HIVE EXTERNE_____
        # On utilise saveAsTable pour la visibilité dans Hive
        # On définit un chemin HDFS pour le Data Lake RAW
        hdfs_path = "hdfs://namenode:9000/lakehouse/raw/instagram_data_raw"

        logger.info("Debut de l'ecriture du RAW")

        df_raw.repartition(8).write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("year", "month", "day","country") \
            .option("path", hdfs_path) \
            .saveAsTable("instagram_data_raw")

        logger.info("Ingestion RAW terminee. Table Hive: {}".format(hdfs_path))

    except Exception as e:
        logger.error("Erreur critique: {}".format(str(e)))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()