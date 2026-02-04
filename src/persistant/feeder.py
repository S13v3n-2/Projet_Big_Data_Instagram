"""
feeder.py - Ingestion Bronze Layer
Projet: Instagram Engagement Optimizer
"""

from datetime import date
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import time

# Initialisation Spark
spark = (
    SparkSession.builder
    .appName("Instagram_Bronze_Feeder")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

print("Demarrage ingestion Bronze")
print("-" * 60)

# PARTIE 1: Ingestion CSV Profils Utilisateurs

print("\nIngestion CSV: instagram.csv")

input_csv_path = "file:///source/instagram.csv"

# Schema explicite pour validation des types
schema_profiles = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("country", StringType(), True),
    StructField("content_type_preference", StringType(), True),
    StructField("preferred_content_theme", StringType(), True),
    StructField("perceived_stress_score", IntegerType(), True),
    StructField("weekly_work_hours", IntegerType(), True),
    StructField("exercise_hours_per_week", IntegerType(), True),
    StructField("income_level", StringType(), True),
    StructField("education_level", StringType(), True)
])

df_profiles = (
    spark.read
    .option("header", "true")
    .schema(schema_profiles)
    .csv(input_csv_path)
)

print("Nombre de lignes chargees:", df_profiles.count())

# Ajout colonnes de partitionnement
today = date.today()
df_profiles_bronze = (
    df_profiles
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source", F.lit("csv_lifestyle"))
    .withColumn("year", F.lit(today.year))
    .withColumn("month", F.lit(today.month))
    .withColumn("day", F.lit(today.day))
)

df_profiles_bronze.cache()

output_profiles = "hdfs://namenode:9000/lakehouse/bronze/instagram_users_profiles"

# Attente initialisation HDFS
time.sleep(60)

# Ecriture partitionnee
(
    df_profiles_bronze
    .repartition(8)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day", "country")
    .parquet(output_profiles)
)

print("Profils ecrits dans HDFS:", output_profiles)


# PARTIE 2: Ingestion PostgreSQL Logs Usage

print("\nIngestion PostgreSQL: usage_logs")

jdbc_url = "jdbc:postgresql://postgres:5432/instagram_db"
jdbc_user = "hive"
jdbc_password = "hivepassword"

# Requete pour extraire logs des 30 derniers jours
query_logs = """
(SELECT 
    user_id,
    daily_active_minutes_instagram,
    user_engagement_score,
    notification_response_rate,
    subscription_status,
    time_on_reels_per_day,
    time_on_stories_per_day,
    last_login_date,
    created_at
 FROM public.usage_logs
 WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
) AS recent_logs
"""

df_logs = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query_logs)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .option("fetchsize", "10000")
    .option("numPartitions", "8")
    .load()
)

print("Nombre de lignes chargees:", df_logs.count())

# Ajout colonnes de partitionnement
df_logs_bronze = (
    df_logs
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source", F.lit("postgresql_logs"))
    .withColumn("year", F.lit(today.year))
    .withColumn("month", F.lit(today.month))
    .withColumn("day", F.lit(today.day))
)

df_logs_bronze.cache()

output_logs = "hdfs://namenode:9000/lakehouse/bronze/instagram_usage_logs"

# Ecriture partitionnee
(
    df_logs_bronze
    .repartition(8)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day", "subscription_status")
    .parquet(output_logs)
)

print("Logs ecrits dans HDFS:", output_logs)


# Validation rapide
print("\nValidation des donnees:")
print("\nRepartition par pays:")
df_profiles_bronze.groupBy("country").count().orderBy(F.desc("count")).show(5)

print("Repartition par statut abonnement:")
df_logs_bronze.groupBy("subscription_status").count().show()

avg_score = df_logs_bronze.select(F.avg("user_engagement_score")).first()[0]
print("Score engagement moyen:", round(avg_score, 2))

# Nettoyage cache
df_profiles_bronze.unpersist()
df_logs_bronze.unpersist()

print("\nIngestion Bronze terminee")
print("Prochaine etape: processor.py pour la couche Silver")

spark.stop()
