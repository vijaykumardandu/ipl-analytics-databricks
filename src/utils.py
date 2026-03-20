"""
utils.py
--------
Shared SparkSession factory and helper utilities.
"""
import os
import logging

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ipl_pipeline")


def get_spark(app_name: str = "IPL-Analytics") -> SparkSession:
    """
    Returns a local SparkSession with Delta Lake support.
    Calling this multiple times returns the existing session.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_dirs(*paths: str) -> None:
    """Create directories if they do not exist."""
    for p in paths:
        os.makedirs(p, exist_ok=True)
