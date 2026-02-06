# -*- coding: utf-8 -*-
import sys
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# --- CONFIGURATION DES LOGS (Exigence: 1pt) ---
log_filename = "/opt/pipeline/logs/silver_logs.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()]
)
logger = logging.getLogger("SilverProcessor")


def main():
    # Paramétrage
    if len(sys.argv) < 2:
        logger.error("Usage: preprocessor.py <input_raw_path>")
        sys.exit(1)

    input_path = sys.argv[1]

    spark = (SparkSession.builder
             .appName("Instagram_Silver_Transform")
             .enableHiveSupport()
             .getOrCreate())

    try:
        logger.info("Lecture des donnees RAW depuis: {}".format(input_path))

        # Lecture depuis le Data Lake RAW
        df_raw = spark.read.parquet(input_path)

        # --- OPTIMISATION: CACHE (Exigence: visible dans Spark UI
        df_raw.cache()
        count_raw = df_raw.count()
        logger.info("Nombre de lignes chargees: {}".format(count_raw))

        # --- VALIDATION
        logger.info("Application des regles de validation...")
        df_clean = df_raw.filter(
            (F.col("user_id").isNotNull()) &  # Regle 1: ID non nul
            (F.col("age") >= 13) &  # Regle 2: Age minimum
            (F.col("self_reported_happiness").between(0, 10)) &  # Regle 3: Score valide
            (F.col("sessions_per_day").isNotNull()) &  # Regle 4: Usage present
            (F.col("gender").isNotNull())  # Regle 5: Genre renseigne
        )

        # --- WINDOW FUNCTION (Exigence: partition by ) ---
        # Exemple: Classement par score d'engagement par pays
        logger.info("Calcul de la Window Function...")
        window_spec = Window.partitionBy("country").orderBy(F.desc("user_engagement_score"))
        df_silver = df_clean.withColumn("engagement_rank", F.row_number().over(window_spec))

        # --- ECRITURE SILVER (Hive/HDF)
        spark.sql("CREATE DATABASE IF NOT EXISTS silver")

        #_____ Creation table global silver
        output_table = "instagram_data_silver_full"
        hdfs_path = "hdfs://namenode:9000/lakehouse/silver/" # Rendre dynamique

        df_silver.repartition(8).write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("year", "month", "day","country") \
            .option("path", hdfs_path + "instagram_data_silver_full") \
            .saveAsTable(output_table)

        logger.info("Traitement Silver termine avec succes vers {}".format(output_table))

        # _____ Creation table silver users_profiles
        try :
            df_silver.printSchema()
            cols_profiles = ["user_id", "age", "gender", "country", "content_type_preference",
                             "preferred_content_theme", "perceived_stress_score", "weekly_work_hours",
                             "exercise_hours_per_week", "year", "month", "day"]  # On garde les partitions

            cols_usage = ["user_id", "daily_active_minutes_instagram", "user_engagement_score",
                          "notification_response_rate", "subscription_status", "time_on_reels_per_day",
                          "last_login_date", "year", "month", "day", "country"]
            df_silver_users_profiles = df_silver.select(*cols_profiles)
            df_silver_users_usage = df_silver.select(*cols_usage)

            df_silver_users_profiles.repartition(8).write \
                .mode("overwrite") \
                .format("parquet") \
                .partitionBy("year", "month", "day", "country") \
                .option("path", hdfs_path + "instagram_data_users_profiles") \
                .saveAsTable("instagram_data_users_profiles")

            df_silver_users_usage.repartition(8).write \
                .mode("overwrite") \
                .format("parquet") \
                .partitionBy("year", "month", "day", "country") \
                .option("path", hdfs_path + "instagram_data_users_usage") \
                .saveAsTable("instagram_data_users_usage")

        except Exception as e:
            logger.error("Erreur durant le traitement Silver: {}".format(str(e)))

    except Exception as e:
        logger.error("Erreur durant le traitement Silver: {}".format(str(e)))
    finally:
        df_raw.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()