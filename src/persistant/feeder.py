from datetime import date
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
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
input_csv_path = "file:///source/instagram_users_lifestyle.csv"

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

time.sleep(60)

(
    df_profiles_bronze
    .repartition(8)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day", "country")
    .parquet(output_profiles)
)

print("Profils ecrits dans HDFS")
print("Ingestion Bronze terminee")

spark.stop()
