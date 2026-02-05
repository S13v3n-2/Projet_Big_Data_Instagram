# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

# Initialisation de la session
spark = SparkSession.builder \
    .appName("Verification_Bronze") \
    .getOrCreate()

print("--- DEBUT DE LA VERIFICATION ---")

try:
    # Lecture des donnees
    path = "hdfs://namenode:9000/lakehouse/raw/instagram_data_raw"
    df = spark.read.parquet(path)

    # 1. Nombre de colonnes
    print("Nombre de colonnes trouvees : " + str(len(df.columns)))

    # 2. Nombre de lignes
    print("Nombre total de lignes : " + str(df.count()))

    # 3. Affichage des 10 premieres lignes (colonnes principales)
    # On selectionne quelques colonnes pour que l'affichage soit lisible
    df.select("user_id", "gender", "urban_rural", "source", "year").show(10)

    # 4. Verification du schema
    df.printSchema()

except Exception as e:
    print("ERREUR LORS DE LA LECTURE : " + str(e))

print("--- FIN DE LA VERIFICATION ---")
spark.stop()