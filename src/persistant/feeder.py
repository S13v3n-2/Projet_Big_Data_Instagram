# -*- coding: utf-8 -*-
from datetime import date
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Initialisation Spark
spark = (
    SparkSession.builder
    .appName("Instagram_Bronze_JDBC_Feeder")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

print("Demarrage de l ingestion Bronze via JDBC...")

# --- 1. LECTURE SQLITE VIA JDBC ---
# Spark utilisera le JAR passe en paramètre au spark-submit
df_db = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:sqlite:/source/instagram_data.db")
    .option("dbtable", "Instagramme_Usage_Logs")
    .option("driver", "org.sqlite.JDBC")
    .load()
)

# --- 2. LECTURE CSV ---
schema_profiles = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("app_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("urban_rural", StringType(), True),
    StructField("employment_status", StringType(), True),
    StructField("relationship_status", StringType(), True),
    StructField("exercise_hours_per_week", DoubleType(), True),
    StructField("diet_quality", StringType(), True),
    StructField("alcohol_frequency", StringType(), True),
    StructField("self_reported_happiness", IntegerType(), True),
    StructField("blood_pressure_systolic", IntegerType(), True),
    StructField("daily_steps_count", IntegerType(), True),
    StructField("hobbies_count", IntegerType(), True),
    StructField("books_read_per_year", IntegerType(), True),
    StructField("travel_frequency_per_year", IntegerType(), True),
    StructField("sessions_per_day", IntegerType(), True),
    StructField("reels_watched_per_day", IntegerType(), True),
    StructField("likes_given_per_day", IntegerType(), True),
    StructField("dms_sent_per_week", IntegerType(), True),
    StructField("ads_viewed_per_day", IntegerType(), True),
    StructField("time_on_feed_per_day", IntegerType(), True),
    StructField("time_on_messages_per_day", IntegerType(), True),
    StructField("followers_count", IntegerType(), True),
    StructField("uses_premium_features", StringType(), True),
    StructField("account_creation_year", IntegerType(), True),
    StructField("average_session_length_minutes", DoubleType(), True),
    StructField("preferred_content_theme", StringType(), True),
    StructField("two_factor_auth_enabled", StringType(), True),
    StructField("linked_accounts_count", IntegerType(), True),
    StructField("user_engagement_score", DoubleType(), True)
])

df_csv = (
    spark.read
    .option("header", "true")
    .schema(schema_profiles)
    .csv("file:///source/instagram.csv")
)

# --- 3. JOINTURE ET ENRICHISSEMENT ---
df_bronze = (
    df_csv.join(df_db, on="user_id", how="left")
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source", F.lit("csv_db_jdbc"))
    .withColumn("year", F.lit(date.today().year))
    .withColumn("month", F.lit(date.today().month))
    .withColumn("day", F.lit(date.today().day))
)

# --- 4. ECRITURE HDFS ---
output_path = "hdfs://namenode:9000/lakehouse/bronze/instagram_users_profiles"

(
    df_bronze
    .repartition(8)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day", "urban_rural")
    .parquet(output_path)
)

print("Ingestion Bronze JDBC terminee avec succes !")
spark.stop()