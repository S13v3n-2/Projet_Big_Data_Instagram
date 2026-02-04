"""
feeder.py - Ingestion Bronze Layer (Parquet uniquement)
Projet: Instagram Engagement Optimizer
Sources: CSV (profils) + PostgreSQL (logs usage)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os