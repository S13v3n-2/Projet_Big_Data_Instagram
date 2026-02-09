import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

log_filename = "/opt/pipeline/logs/gold_logs.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_filename,
    filemode='a'
)

logger = logging.getLogger("GoldDatamart")

def init_spark():
    return (
        SparkSession.builder
        .appName("Instagram_Gold_Datamarts")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar")
        .enableHiveSupport()
        .getOrCreate()
    )

def write_postgres(df, table_name, jdbc_url, props):
    df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=props)
    logger.info("Table {} ecrite dans PostgreSQL".format(table_name))

def main():
    if len(sys.argv) < 2:
        logger.error("Usage: datamart.py <silver_path>")
        sys.exit(1)
    
    silver_path = sys.argv[1]
    
    spark = init_spark()
    
    jdbc_url = "jdbc:postgresql://postgres-instagram:5432/instagram_db"
    props = {
        "user": "admin",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    
    try:
        logger.info("Lecture Silver depuis: {}".format(silver_path))
        df_silver = spark.read.parquet(silver_path)
        
        logger.info("Calcul moyenne globale engagement")
        global_avg = df_silver.agg(F.avg("user_engagement_score")).collect()[0][0]
        
        logger.info("Datamart 1: engagement_by_content")
        df_engagement = (
            df_silver
            .withColumn("age_range", 
                F.when(F.col("age") < 25, "18-24")
                .when(F.col("age") < 35, "25-34")
                .when(F.col("age") < 45, "35-44")
                .otherwise("45+"))
            .withColumn("lifestyle_segment",
                F.when((F.col("exercise_hours_per_week") > 5) & (F.col("self_reported_happiness") > 7), "Fit")
                .when(F.col("sessions_per_day") > 10, "Workaholic")
                .when(F.col("sessions_per_day") < 3, "Sleep")
                .otherwise("Balanced"))
            .groupBy("country", "age_range", "lifestyle_segment", 
                    "content_type_preference", "preferred_content_theme")
            .agg(
                F.avg("user_engagement_score").alias("avg_engagement_score"),
                F.count("user_id").alias("total_users"),
                F.sum(F.when(F.col("sessions_per_day") == 0, 1).otherwise(0)).alias("churned_users")
            )
            .withColumn("churn_rate", F.col("churned_users") / F.col("total_users") * 100)
            .withColumn("engagement_gain_pct", 
                (F.col("avg_engagement_score") - global_avg) / global_avg * 100)
            .withColumn("segment_id", 
                F.concat_ws("-", F.col("country"), F.col("age_range"), 
                           F.col("lifestyle_segment"), F.col("content_type_preference"),
                           F.col("preferred_content_theme")))
        )
        
        write_postgres(df_engagement, "engagement_by_content", jdbc_url, props)
        
        logger.info("Datamart 2: user_segmentation")
        df_user_seg = (
            df_silver
            .withColumn("lifestyle_segment",
                F.when((F.col("exercise_hours_per_week") > 5) & (F.col("self_reported_happiness") > 7), "Fit")
                .when(F.col("sessions_per_day") > 10, "Workaholic")
                .when(F.col("sessions_per_day") < 3, "Sleep")
                .otherwise("Balanced"))
            .withColumn("persona_cluster",
                F.when(F.col("lifestyle_segment") == "Fit", 0)
                .when(F.col("lifestyle_segment") == "Workaholic", 1)
                .when(F.col("lifestyle_segment") == "Sleep", 2)
                .otherwise(3))
            .withColumn("churn_probability",
                F.when(F.col("sessions_per_day") < 2, 0.8)
                .when(F.col("sessions_per_day") < 5, 0.5)
                .otherwise(0.2))
            .withColumn("top_content_recommendation",
                F.concat_ws("-", F.col("content_type_preference"), F.col("preferred_content_theme")))
            .withColumn("lifetime_value_estimate",
                F.col("user_engagement_score") * 0.5 + F.col("sessions_per_day") * 10)
            .select(
                "user_id",
                "persona_cluster",
                F.col("lifestyle_segment").alias("persona_name"),
                F.col("user_engagement_score").alias("predicted_engagement"),
                "top_content_recommendation",
                "churn_probability",
                "lifetime_value_estimate"
            )
        )
        
        write_postgres(df_user_seg, "user_segmentation", jdbc_url, props)
        
        logger.info("Datamart 3: content_performance")
        window_rank = Window.partitionBy("content_type_preference").orderBy(F.desc("avg_engagement_score"))
        
        df_content_perf = (
            df_silver
            .groupBy("content_type_preference", "preferred_content_theme")
            .agg(
                F.count("user_id").alias("total_users_preferring"),
                F.avg("user_engagement_score").alias("avg_engagement_score"),
                F.avg("sessions_per_day").alias("avg_daily_minutes"),
                F.first("country").alias("top_country")
            )
            .withColumn("rank_in_type", F.row_number().over(window_rank))
            .withColumnRenamed("content_type_preference", "content_type")
            .withColumnRenamed("preferred_content_theme", "content_theme")
        )
        
        write_postgres(df_content_perf, "content_performance", jdbc_url, props)
        
        logger.info("Datamart 4: lifestyle_impact")
        df_lifestyle = (
            df_silver
            .withColumn("lifestyle_segment",
                F.when((F.col("exercise_hours_per_week") > 5) & (F.col("self_reported_happiness") > 7), "Fit")
                .when(F.col("sessions_per_day") > 10, "Workaholic")
                .when(F.col("sessions_per_day") < 3, "Sleep")
                .otherwise("Balanced"))
            .groupBy("lifestyle_segment", "content_type_preference")
            .agg(
                F.avg("self_reported_happiness").alias("avg_stress_score"),
                F.avg("sessions_per_day").alias("avg_work_hours"),
                F.avg("user_engagement_score").alias("avg_engagement"),
                F.sum(F.when(F.col("sessions_per_day") > 10, 1).otherwise(0)).alias("over_usage_count"),
                F.count("user_id").alias("total_users")
            )
            .withColumn("over_usage_pct", F.col("over_usage_count") / F.col("total_users") * 100)
        )
        
        write_postgres(df_lifestyle, "lifestyle_impact", jdbc_url, props)
        
        logger.info("Tous les datamarts Gold crees avec succes")
        
    except Exception as e:
        logger.error("Erreur: {}".format(str(e)))
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
