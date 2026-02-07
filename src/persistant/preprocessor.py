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
    filename=log_filename, # Redirige les flux vers le fichier directement
    filemode='a'            # 'a' pour ajouter au fichier, 'w' pour écraser à chaque run
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
        database = "silver"
        spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database))
        spark.sql("USE {}".format(database))

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
            cols_profiles = ["user_id", "age", "gender", "country", "urban_rural","income_level", "education_level",
                             "employment_status", "relationship_status","has_children","weekly_work_hours",
                             "sleep_hours_per_night","exercise_hours_per_week","body_mass_index","perceived_stress_score",
                             "self_reported_happiness","year", "month", "day"
        ]

            cols_usage = ["user_id","daily_active_minutes_instagram","user_engagement_score","sessions_per_day",
                          "average_session_length_minutes","reels_watched_per_day","time_on_reels_per_day",
                          "ads_viewed_per_day","ads_clicked_per_day","last_login_date","notification_response_rate",
                          "subscription_status","content_type_preference","preferred_content_theme","engagement_rank",
                          "year", "month", "day", "country"
]
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

        try:
            logger.info("Debut de la creation du DataFrame enriched")

            # 1. Chargement des tables Silver
            df_profiles = spark.table("instagram_data_users_profiles")
            df_usage = spark.table("instagram_data_users_usage")

            # Jointure sur user_id
            df_base = df_profiles.join(df_usage.drop("year", "month", "day", "country"), "user_id", "inner")

            # 2. Application de la logique Business
            df_enriched = df_base \
                .withColumn("engagement_rate_per_minute",
                            F.when(F.col("daily_active_minutes_instagram") > 0,
                                   F.col("user_engagement_score") / F.col("daily_active_minutes_instagram"))
                            .otherwise(0)) \
                .withColumn("lifestyle_segment",
                            F.when(F.col("weekly_work_hours") > 50, "Workaholic")
                            .when(F.col("sleep_hours_per_night") < 6, "Sleep Deprived")
                            .when(F.col("exercise_hours_per_week") > 5, "Fit Relaxed")
                            .otherwise("Balanced")) \
                .withColumn("work_life_balance_index",
                            F.when(F.col("exercise_hours_per_week") > 0,
                                   (168 - F.col("weekly_work_hours")) / F.col("exercise_hours_per_week"))
                            .otherwise(0)) \
                .withColumn("digital_wellbeing_score",
                            F.greatest(F.lit(0), F.lit(100) - (F.col("sessions_per_day") * 5))) \
                .withColumn("days_since_last_login",
                            F.datediff(F.current_date(), F.to_date(F.col("last_login_date")))) \
                .withColumn("churn_risk_flag",
                            F.when(F.col("days_since_last_login") > 90, True).otherwise(False))

            # 3. Window Function
            # Moyenne d'engagement par segment de lifestyle pour comparer l'individu au groupe
            window_lifestyle = Window.partitionBy("lifestyle_segment")
            df_final = df_enriched.withColumn("avg_segment_engagement",
                                              F.avg("user_engagement_score").over(window_lifestyle))

            # 4. OPTIMISATION : Persist
            df_final.persist()
            logger.info("Persist active pour df_final. Lignes : {}".format(df_final.count()))

            table = "instagram_data_users_enriched"

            # 5. ECRITURE FINALE [cite: 30, 31, 32]
            df_final.repartition(8).write \
                .mode("overwrite") \
                .format("parquet") \
                .partitionBy("year", "month", "day") \
                .option("path", hdfs_path + "instagram_data_users_enriched") \
                .saveAsTable(table)

            logger.info("Table {} sauvegardee avec succes").format(table)

        except Exception as e:
            logger.error("Erreur dans le calcul enriched : {}".format(str(e)))

    except Exception as e:
        logger.error("Erreur durant le traitement Silver: {}".format(str(e)))
    finally:
        df_raw.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()