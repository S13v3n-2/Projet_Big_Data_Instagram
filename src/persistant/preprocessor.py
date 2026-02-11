# -*- coding: utf-8 -*-
import sys
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark import StorageLevel

# --- CONFIGURATION DES LOGS ---
# On écrit directement dans le fichier de log
log_filename = "/opt/pipeline/logs/silver_logs.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_filename,
    filemode='a'
)
logger = logging.getLogger("SilverProcessor")


def main():
    # 1. Validation des arguments pour éviter les chemins en dur
    if len(sys.argv) < 2:
        logger.error("Usage: preprocessor.py <input_raw_path>")
        sys.exit(1)

    input_path = sys.argv[1]  # Ex: hdfs://namenode:9000/lakehouse/raw/instagram_data_raw
    hdfs_root = "hdfs://namenode:9000/lakehouse"

    spark = (SparkSession.builder
             .appName("Instagram_Silver_Transform")
             .enableHiveSupport()
             .getOrCreate())

    try:
        logger.info("Lecture des donnees RAW depuis: {}".format(input_path))
        df_raw = spark.read.parquet(input_path)

        # 2. VALIDATION (Les 5 règles métier)
        logger.info("Application des regles de validation...")
        df_clean = df_raw.filter(
            (F.col("user_id").isNotNull()) &  # Regle 1: ID non nul
            (F.col("age") >= 13) &  # Regle 2: Age minimum
            (F.col("self_reported_happiness").between(0, 10)) &  # Regle 3: Score valide
            (F.col("sessions_per_day").isNotNull()) &  # Regle 4: Usage present
            (F.col("gender").isNotNull())  # Regle 5: Genre renseigne
        )

        #CALCUL DU RANG (Window Function)
        logger.info("Calcul du rang d'engagement par pays...")
        window_country = Window.partitionBy("country").orderBy(F.desc("user_engagement_score"))
        df_silver = df_clean.withColumn("engagement_rank", F.row_number().over(window_country))

        # On persiste ce DataFrame car il va servir de source pour TOUTES les tables (Enriched, Profiles, Usage)
        # Cela évite de relire le fichier RAW 3 fois.
        df_silver.persist(StorageLevel.MEMORY_AND_DISK)
        count_silver = df_silver.count()
        logger.info("Donnees Silver valides et persistees. Lignes : {}".format(count_silver))

        # Configuration de la base de données Hive
        database = "silver"
        spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database))
        spark.sql("USE {}".format(database))

        # ====================================================
        # CREATION DE LA TABLE ENRICHIE (KPIs Métier)
        # ====================================================
        # On calcule cela DIRECTEMENT depuis df_silver, sans jointure
        logger.info("Calcul des KPIs pour la table Enriched...")

        df_enriched = df_silver \
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

        # Window Function supplémentaire pour l'enrichissement
        window_lifestyle = Window.partitionBy("lifestyle_segment")
        df_final_enriched = df_enriched.withColumn("avg_segment_engagement",
                                                   F.avg("user_engagement_score").over(window_lifestyle))

        # Ecriture de la table ENRICHED (La plus importante pour le Gold)
        path_enriched = "{}/silver/instagram_data_users_enriched".format(hdfs_root)
        df_final_enriched.repartition(8).write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("year", "month", "day") \
            .option("path", path_enriched) \
            .saveAsTable("instagram_data_users_enriched")

        logger.info("Table 'instagram_data_users_enriched' sauvegardee.")

        # ====================================================
        # CREATION DES TABLES PROFILES ET USAGE
        # ====================================================
        # On projette simplement les colonnes depuis df_silver (toujours en cache)

        # Liste des colonnes Profiles (Incluant celles nécessaires aux futurs calculs)
        cols_profiles = [
            "user_id", "age", "gender", "country", "urban_rural", "income_level",
            "education_level", "employment_status", "relationship_status", "has_children",
            "weekly_work_hours", "sleep_hours_per_night", "exercise_hours_per_week",
            "body_mass_index", "perceived_stress_score", "self_reported_happiness",
            "year", "month", "day"
        ]

        # Liste des colonnes Usage
        cols_usage = [
            "user_id", "daily_active_minutes_instagram", "user_engagement_score",
            "sessions_per_day", "average_session_length_minutes", "reels_watched_per_day",
            "time_on_reels_per_day", "ads_viewed_per_day", "ads_clicked_per_day",
            "last_login_date", "notification_response_rate", "subscription_status",
            "content_type_preference", "preferred_content_theme", "engagement_rank",
            "year", "month", "day", "country"
        ]

        # Ecriture User Profiles
        path_profiles = "{}/silver/instagram_data_users_profiles".format(hdfs_root)
        df_silver.select(*cols_profiles).repartition(8).write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("year", "month", "day", "country") \
            .option("path", path_profiles) \
            .saveAsTable("instagram_data_users_profiles")

        logger.info("Table 'instagram_data_users_profiles' sauvegardee.")

        # Ecriture User Usage
        path_usage = "{}/silver/instagram_data_users_usage".format(hdfs_root)
        df_silver.select(*cols_usage).repartition(8).write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("year", "month", "day", "country") \
            .option("path", path_usage) \
            .saveAsTable("instagram_data_users_usage")

        logger.info("Table 'instagram_data_users_usage' sauvegardee.")

        logger.info("Traitement Silver complet termine avec succes.")

    except Exception as e:
        logger.error("Erreur critique durant le traitement Silver: {}".format(str(e)))
        sys.exit(1)
    finally:
        # Nettoyage de la mémoire
        if 'df_silver' in locals():
            df_silver.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()