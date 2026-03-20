"""
bronze_ingest.py
----------------
Stage 1 — Bronze Layer

Reads raw matches.csv and deliveries.csv and writes them as Delta tables
with zero transformation — just ingestion metadata added.

Run:
    python src/bronze_ingest.py
"""
import os
import sys
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    MATCHES_PATH, DELIVERIES_PATH,
    BRONZE_MATCHES_PATH, BRONZE_DELIVERIES_PATH,
)
from src.utils import get_spark, ensure_dirs, logger


def ingest_matches(spark) -> int:
    logger.info("Reading matches: %s", MATCHES_PATH)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(MATCHES_PATH)
        .withColumn("ingested_at",   F.current_timestamp())
        .withColumn("pipeline_date", F.current_date())
    )
    count = df.count()
    logger.info("  Matches read: %d", count)

    ensure_dirs(BRONZE_MATCHES_PATH)
    df.write.format("delta").mode("overwrite").save(BRONZE_MATCHES_PATH)
    logger.info("  Bronze matches written → %s", BRONZE_MATCHES_PATH)
    return count


def ingest_deliveries(spark) -> int:
    logger.info("Reading deliveries: %s", DELIVERIES_PATH)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(DELIVERIES_PATH)
        .withColumn("ingested_at",   F.current_timestamp())
        .withColumn("pipeline_date", F.current_date())
    )
    count = df.count()
    logger.info("  Deliveries read: %d", count)

    ensure_dirs(BRONZE_DELIVERIES_PATH)
    df.write.format("delta").mode("overwrite").save(BRONZE_DELIVERIES_PATH)
    logger.info("  Bronze deliveries written → %s", BRONZE_DELIVERIES_PATH)
    return count


def run_bronze() -> None:
    spark = get_spark("Bronze-Ingest")

    m_count = ingest_matches(spark)
    d_count = ingest_deliveries(spark)

    logger.info("Bronze layer complete.")
    logger.info("  Matches   : %d rows", m_count)
    logger.info("  Deliveries: %d rows", d_count)


if __name__ == "__main__":
    run_bronze()
