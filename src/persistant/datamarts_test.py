# -*- coding: utf-8 -*-
import sys
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# --- CONFIGURATION DES LOGS (Version simplifiée sans handlers) ---
log_filename = "/opt/pipeline/logs/gold_logs.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_filename,
    filemode='a'
)
logger = logging.getLogger("GoldDatamart")

def init_spark():
    """Initialise Spark avec support Hive et JDBC PostgreSQL."""
    return (SparkSession.builder
            .appName("Instagram_Gold_Datamart_Final")
            .enableHiveSupport()
            .getOrCreate())


def save_to_postgres(df, table_name, url, props):
    """Ecriture securisee dans PostgreSQL."""
    try:
        # On utilise repartition(1) pour l'ecriture JDBC afin d'eviter trop de connexions simultanees
        df.repartition(1).write.jdbc(url=url, table=table_name, mode="overwrite", properties=props)
        logger.info("Table Gold '{}' exportee avec succes.".format(table_name))
    except Exception as e:
        logger.error("Erreur export PostgreSQL sur {}: {}".format(table_name, str(e)))


def main():
    # Verification des arguments : on attend le nom de la table Silver (ex: silver.instagram_data_users_enriched)
    if len(sys.argv) < 2:
        logger.error("Usage: datamart.py <silver_table_name>")
        sys.exit(1)

    table_source = sys.argv[1]
    spark = init_spark()

    # Configuration de la base de donnees de destination (API)
    jdbc_url = "jdbc:postgresql://postgres-instagram:5432/instagram_db"
    db_properties = {
        "user": "admin",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    try:
        logger.info("Lecture de la table source : {}".format(table_source))
        df_silver = spark.table(table_source)

        # ___ OPTIMISATION AVEC PERSISTANCE DES DONNEES EN CACHE ___
        df_silver.persist()
        logger.info("Persist active sur la source Silver ({} lignes)".format(df_silver.count()))

        # 1. DATAMART : ANALYSE DE L'ENGAGEMENT
        # On utilise les colonnes deja calculees dans ta table enrichie
        logger.info("Generation Datamart Engagement...")
        df_engagement = df_silver.groupBy("country", "lifestyle_segment", "content_type_preference") \
            .agg(
            F.avg("user_engagement_score").alias("avg_engagement"),
            F.avg("engagement_rate_per_minute").alias("avg_efficiency"),
            F.count("user_id").alias("total_users")
        )
        save_to_postgres(df_engagement, "gold_engagement_stats", jdbc_url, db_properties)

        # 2. DATAMART : RISQUE DE CHURN ET BIEN-ETRE (Pour Graphique 2)
        logger.info("Generation Datamart Churn & Wellbeing...")
        df_wellbeing = df_silver.groupBy("lifestyle_segment") \
            .agg(
            F.avg("digital_wellbeing_score").alias("avg_wellbeing_score"),
            F.avg("days_since_last_login").alias("avg_days_inactive"),
            F.sum(F.col("churn_risk_flag").cast("int")).alias("potential_churners")
        )
        save_to_postgres(df_wellbeing, "gold_user_health", jdbc_url, db_properties)

        # 3. DATAMART : TOP CONTENUS PAR SEGMENT (Pour l'API / Graphique 3)
        logger.info("Generation Datamart Content Performance...")
        # On utilise le rang deja calcule par ta Window Function en Silver
        df_content = df_silver.filter(F.col("engagement_rank") <= 10) \
            .select("user_id", "country", "lifestyle_segment", "content_type_preference", "engagement_rank")

        save_to_postgres(df_content, "gold_top_recommendations", jdbc_url, db_properties)

        logger.info("Tous les Datamarts ont ete mis a jour dans PostgreSQL.")

    except Exception as e:
        logger.error("Erreur durant le traitement Gold : {}".format(str(e)))
    finally:
        df_silver.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()