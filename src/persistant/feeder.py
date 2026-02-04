from datetime import date
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import sqlite3
import time

spark = (
    SparkSession.builder
    .appName("Instagram_Bronze_Feeder")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

print("Demarrage ingestion Bronze")

# Ingestion CSV Profils
input_csv_path = "file:///source/instagram.csv"

schema_profiles = StructType([
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

df_profiles = (
    spark.read
    .option("header", "true")
    .schema(schema_profiles)
    .csv(input_csv_path)
)

print("Nombre de lignes CSV:", df_profiles.count())

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

#time.sleep(60)
(
    df_profiles_bronze
    .repartition(8)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day", "urban_rural")
    .parquet(output_profiles)
)

print("Profils ecrits dans HDFS")
print("Ingestion Bronze terminee")

spark.stop()
