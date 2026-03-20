"""
analytics_queries.py
--------------------
Stage 4 — SQL-style analytical queries on Gold tables.

Mirrors what you would run in the Databricks SQL Editor.
Prints formatted results to console — screenshot these for your portfolio.

Run:
    python src/analytics_queries.py
"""
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    GOLD_BATSMAN_PATH, GOLD_BOWLER_PATH,
    GOLD_TEAM_PATH, GOLD_MATCH_SUMMARY_PATH,
)
from src.utils import get_spark, logger

DIVIDER = "─" * 55


def run_queries() -> None:
    spark = get_spark("Analytics-Queries")

    batsman  = spark.read.format("delta").load(GOLD_BATSMAN_PATH)
    bowler   = spark.read.format("delta").load(GOLD_BOWLER_PATH)
    team     = spark.read.format("delta").load(GOLD_TEAM_PATH)
    matches  = spark.read.format("delta").load(GOLD_MATCH_SUMMARY_PATH)

    # ── Query 1: Top 10 run scorers ───────────────────────────────────────────
    print(f"\n{DIVIDER}")
    print("  Query 1 — Top 10 all-time run scorers")
    print(DIVIDER)
    batsman.filter(F.col("overall_rank") <= 10) \
           .select("overall_rank", "batsman", "total_runs",
                   "strike_rate", "batting_average", "performance_tier") \
           .orderBy("overall_rank") \
           .show(truncate=False)

    # ── Query 2: Best economy bowlers ─────────────────────────────────────────
    print(f"\n{DIVIDER}")
    print("  Query 2 — Top 10 bowlers by economy rate")
    print(DIVIDER)
    bowler.filter(F.col("economy_rank") <= 10) \
          .select("economy_rank", "bowler", "total_wickets",
                  "economy_rate", "bowling_average", "dot_ball_pct") \
          .orderBy("economy_rank") \
          .show(truncate=False)

    # ── Query 3: Rolling 3-season wins per team ───────────────────────────────
    print(f"\n{DIVIDER}")
    print("  Query 3 — Rolling 3-season win total per team (last 5 seasons)")
    print(DIVIDER)
    team.filter(F.col("season") >= 2019) \
        .select("season", "winner", "wins", "rolling_3season_wins", "season_rank") \
        .orderBy("season", "season_rank") \
        .show(40, truncate=False)

    # ── Query 4: Toss decision analysis ──────────────────────────────────────
    print(f"\n{DIVIDER}")
    print("  Query 4 — Toss decision win rate (bat vs field)")
    print(DIVIDER)
    matches.groupBy("toss_decision") \
           .agg(
               F.count("id").alias("total_matches"),
               F.sum(F.col("toss_winner_won").cast("int")).alias("toss_winner_won"),
               F.round(
                   F.sum(F.col("toss_winner_won").cast("int")) /
                   F.count("id") * 100, 2
               ).alias("win_pct_after_toss")
           ) \
           .show(truncate=False)

    # ── Query 5: Top venue by avg score ──────────────────────────────────────
    print(f"\n{DIVIDER}")
    print("  Query 5 — Top 5 highest-scoring venues")
    print(DIVIDER)
    matches.filter(F.col("innings1_runs").isNotNull()) \
           .groupBy("venue") \
           .agg(
               F.round(F.avg("innings1_runs"), 1).alias("avg_1st_innings"),
               F.count("id").alias("matches_hosted"),
               F.round(F.max("innings1_runs"), 0).alias("highest_score"),
           ) \
           .filter(F.col("matches_hosted") >= 5) \
           .orderBy(F.desc("avg_1st_innings")) \
           .show(5, truncate=False)

    # ── Query 6: Season-wise all-rounder (runs + wickets) ────────────────────
    print(f"\n{DIVIDER}")
    print("  Query 6 — Top all-rounders (500+ runs AND 50+ wickets)")
    print(DIVIDER)
    all_rounders = batsman.join(
        bowler,
        batsman["batsman"] == bowler["bowler"],
        "inner"
    ).filter(
        (F.col("total_runs") >= 500) &
        (F.col("total_wickets") >= 50)
    ).select(
        batsman["batsman"].alias("player"),
        "total_runs", "batting_average", "strike_rate",
        "total_wickets", "economy_rate"
    ).orderBy(F.desc("total_runs"))
    all_rounders.show(truncate=False)

    print(f"\n{DIVIDER}")
    print("  All queries complete. Screenshot the outputs above.")
    print(DIVIDER)


if __name__ == "__main__":
    run_queries()
