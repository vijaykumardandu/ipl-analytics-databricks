"""
silver_transform.py
-------------------
Stage 2 — Silver Layer

Reads from Bronze Delta tables and applies:
  Matches  : null filtering, type casting, season extraction, playoff flag
  Deliveries: null filtering, four boolean indicator columns, deduplication

Run:
    python src/silver_transform.py
"""
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    BRONZE_MATCHES_PATH, BRONZE_DELIVERIES_PATH,
    SILVER_MATCHES_PATH, SILVER_DELIVERIES_PATH,
)
from src.utils import get_spark, ensure_dirs, logger


# ── Matches transforms ────────────────────────────────────────────────────────

def transform_matches(spark):
    logger.info("Transforming matches …")
    df = spark.read.format("delta").load(BRONZE_MATCHES_PATH)

    cleaned = (
        df
        # remove rain-affected no-result games for analytics clarity
        .filter(F.col("result") != "no result")
        # extract numeric season safely
        .withColumn("season", F.col("season").cast("int"))
        # parse match date
        .withColumn("match_date", F.to_date("date", "yyyy-MM-dd"))
        # is this match a knockout stage game?
        .withColumn(
            "is_playoff",
            F.col("match_type").isin(["Final", "Qualifier", "Eliminator"])
        )
        # toss advantage flag
        .withColumn(
            "toss_winner_won",
            F.col("toss_winner") == F.col("winner")
        )
        # remove duplicates
        .dropDuplicates(["id"])
        .withColumn("processed_at", F.current_timestamp())
    )

    count = cleaned.count()
    logger.info("  Silver matches: %d rows", count)

    ensure_dirs(SILVER_MATCHES_PATH)
    cleaned.write.format("delta").mode("overwrite").save(SILVER_MATCHES_PATH)
    logger.info("  Saved → %s", SILVER_MATCHES_PATH)
    return count


# ── Deliveries transforms ─────────────────────────────────────────────────────

def transform_deliveries(spark):
    logger.info("Transforming deliveries …")
    df = spark.read.format("delta").load(BRONZE_DELIVERIES_PATH)

    enriched = (
        df
        # drop rows with missing core fields
        .filter(F.col("batsman").isNotNull())
        .filter(F.col("bowler").isNotNull())
        .filter(F.col("batsman_runs").isNotNull())

        # ── boolean indicator columns (Silver enrichment) ─────────────────
        .withColumn(
            "is_boundary",
            F.col("batsman_runs").isin([4, 6]).cast("boolean")
        )
        .withColumn(
            "is_six",
            (F.col("batsman_runs") == 6).cast("boolean")
        )
        .withColumn(
            "is_four",
            (F.col("batsman_runs") == 4).cast("boolean")
        )
        .withColumn(
            "is_dot_ball",
            (
                (F.col("batsman_runs") == 0) &
                (F.col("extra_runs") == 0)
            ).cast("boolean")
        )
        .withColumn(
            "is_wicket_delivery",
            F.col("player_dismissed").isNotNull().cast("boolean")
        )
        .withColumn(
            "is_wide",
            (F.col("wide_runs") > 0).cast("boolean")
        )
        .withColumn(
            "is_noball",
            (F.col("noball_runs") > 0).cast("boolean")
        )

        # ── deduplication ─────────────────────────────────────────────────
        .dropDuplicates(["match_id", "inning", "over", "ball", "batsman", "bowler"])

        .withColumn("processed_at", F.current_timestamp())
    )

    count = enriched.count()
    logger.info("  Silver deliveries: %d rows", count)

    ensure_dirs(SILVER_DELIVERIES_PATH)
    enriched.write.format("delta").mode("overwrite").save(SILVER_DELIVERIES_PATH)
    logger.info("  Saved → %s", SILVER_DELIVERIES_PATH)
    return count


def run_silver() -> None:
    spark = get_spark("Silver-Transform")
    m = transform_matches(spark)
    d = transform_deliveries(spark)
    logger.info("Silver layer complete. Matches=%d  Deliveries=%d", m, d)


if __name__ == "__main__":
    run_silver()
