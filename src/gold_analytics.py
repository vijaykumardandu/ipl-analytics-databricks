"""
gold_analytics.py
-----------------
Stage 3 — Gold Layer (Business Analytics)

Produces four Gold tables from Silver data:
  1. batsman_stats       — career batting metrics + performance tier ranking
  2. bowler_stats        — career bowling metrics + economy rank
  3. team_season_wins    — wins per team per season + rolling 3-season total
  4. match_summary       — enriched match-level summary with toss and winner info

Run:
    python src/gold_analytics.py
"""
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    SILVER_MATCHES_PATH, SILVER_DELIVERIES_PATH,
    GOLD_BATSMAN_PATH, GOLD_BOWLER_PATH,
    GOLD_TEAM_PATH, GOLD_MATCH_SUMMARY_PATH,
    MIN_MATCHES_BATSMAN, MIN_MATCHES_BOWLER, MIN_WICKETS_BOWLER,
)
from src.utils import get_spark, ensure_dirs, logger


# ── 1. Batsman career stats ───────────────────────────────────────────────────

def build_batsman_stats(spark):
    logger.info("Building batsman stats …")
    df = spark.read.format("delta").load(SILVER_DELIVERIES_PATH)

    # aggregate per batsman
    stats = (
        df
        .filter(F.col("is_wide") == False)   # wides don't count as batsman delivery
        .groupBy("batsman")
        .agg(
            F.sum("batsman_runs")                   .alias("total_runs"),
            F.count("ball")                         .alias("balls_faced"),
            F.sum(F.col("is_boundary").cast("int")) .alias("boundaries"),
            F.sum(F.col("is_six").cast("int"))      .alias("sixes"),
            F.sum(F.col("is_four").cast("int"))     .alias("fours"),
            F.sum(F.col("is_dot_ball").cast("int")) .alias("dot_balls"),
            F.countDistinct("match_id")             .alias("matches_played"),
            F.sum(F.col("is_wicket_delivery")
                  .cast("int"))                     .alias("times_dismissed"),
        )
        .filter(F.col("matches_played") >= MIN_MATCHES_BATSMAN)
        # derived metrics
        .withColumn("strike_rate",
            F.round(F.col("total_runs") / F.col("balls_faced") * 100, 2))
        .withColumn("batting_average",
            F.round(
                F.when(F.col("times_dismissed") == 0, F.col("total_runs"))
                 .otherwise(F.col("total_runs") / F.col("times_dismissed")), 2
            )
        )
        .withColumn("avg_runs_per_match",
            F.round(F.col("total_runs") / F.col("matches_played"), 2))
        .withColumn("boundary_pct",
            F.round(F.col("boundaries") / F.col("balls_faced") * 100, 2))
    )

    # window-based overall rank
    w_rank = Window.orderBy(F.desc("total_runs"))
    ranked = (
        stats
        .withColumn("overall_rank", F.rank().over(w_rank))
        .withColumn("performance_tier",
            F.when(F.col("overall_rank") <= 10, "Elite")
             .when(F.col("overall_rank") <= 30, "Top Performer")
             .when(F.col("overall_rank") <= 60, "Regular")
             .otherwise("Occasional"))
        .withColumn("gold_created_at", F.current_timestamp())
    )

    count = ranked.count()
    logger.info("  Batsman stats: %d players", count)
    ensure_dirs(GOLD_BATSMAN_PATH)
    ranked.write.format("delta").mode("overwrite").save(GOLD_BATSMAN_PATH)
    logger.info("  Saved → %s", GOLD_BATSMAN_PATH)

    # show top 10
    logger.info("\n  ── Top 10 Batsmen ──")
    ranked.filter(F.col("overall_rank") <= 10) \
          .select("overall_rank", "batsman", "total_runs",
                  "strike_rate", "batting_average", "performance_tier") \
          .orderBy("overall_rank") \
          .show(truncate=False)
    return count


# ── 2. Bowler career stats ────────────────────────────────────────────────────

def build_bowler_stats(spark):
    logger.info("Building bowler stats …")
    df = spark.read.format("delta").load(SILVER_DELIVERIES_PATH)

    stats = (
        df
        .groupBy("bowler")
        .agg(
            F.sum(F.col("is_wicket_delivery").cast("int")).alias("total_wickets"),
            F.sum("total_runs")                           .alias("runs_conceded"),
            F.count("ball")                               .alias("balls_bowled"),
            F.sum(F.col("is_dot_ball").cast("int"))       .alias("dot_balls"),
            F.sum(F.col("is_wide").cast("int"))           .alias("wides"),
            F.sum(F.col("is_noball").cast("int"))         .alias("noballs"),
            F.countDistinct("match_id")                   .alias("matches"),
        )
        .filter(F.col("matches") >= MIN_MATCHES_BOWLER)
        .filter(F.col("total_wickets") >= MIN_WICKETS_BOWLER)
        # derived bowling metrics
        .withColumn("economy_rate",
            F.round(F.col("runs_conceded") / (F.col("balls_bowled") / 6), 2))
        .withColumn("bowling_average",
            F.round(F.col("runs_conceded") / F.col("total_wickets"), 2))
        .withColumn("bowling_strike_rate",
            F.round(F.col("balls_bowled") / F.col("total_wickets"), 2))
        .withColumn("dot_ball_pct",
            F.round(F.col("dot_balls") / F.col("balls_bowled") * 100, 2))
    )

    # economy rank (lower = better)
    w_eco = Window.orderBy("economy_rate")
    w_wkt = Window.orderBy(F.desc("total_wickets"))
    ranked = (
        stats
        .withColumn("economy_rank", F.rank().over(w_eco))
        .withColumn("wickets_rank", F.rank().over(w_wkt))
        .withColumn("gold_created_at", F.current_timestamp())
    )

    count = ranked.count()
    logger.info("  Bowler stats: %d bowlers", count)
    ensure_dirs(GOLD_BOWLER_PATH)
    ranked.write.format("delta").mode("overwrite").save(GOLD_BOWLER_PATH)
    logger.info("  Saved → %s", GOLD_BOWLER_PATH)

    logger.info("\n  ── Top 10 Wicket Takers ──")
    ranked.filter(F.col("wickets_rank") <= 10) \
          .select("wickets_rank", "bowler", "total_wickets",
                  "economy_rate", "dot_ball_pct") \
          .orderBy("wickets_rank") \
          .show(truncate=False)
    return count


# ── 3. Team season wins ───────────────────────────────────────────────────────

def build_team_season_wins(spark):
    logger.info("Building team season wins …")
    matches = spark.read.format("delta").load(SILVER_MATCHES_PATH)

    wins = (
        matches
        .filter(F.col("winner").isNotNull() & (F.col("winner") != ""))
        .groupBy("season", "winner")
        .agg(F.count("id").alias("wins"))
        .orderBy("winner", "season")
    )

    # rolling 3-season wins window per team
    w_roll = (
        Window.partitionBy("winner")
        .orderBy("season")
        .rowsBetween(-2, 0)
    )

    # season rank within each season
    w_season = Window.partitionBy("season").orderBy(F.desc("wins"))

    enriched = (
        wins
        .withColumn("rolling_3season_wins", F.sum("wins").over(w_roll))
        .withColumn("season_rank",          F.rank().over(w_season))
        .withColumn("gold_created_at",      F.current_timestamp())
    )

    count = enriched.count()
    logger.info("  Team season rows: %d", count)
    ensure_dirs(GOLD_TEAM_PATH)
    enriched.write.format("delta").mode("overwrite").save(GOLD_TEAM_PATH)
    logger.info("  Saved → %s", GOLD_TEAM_PATH)

    logger.info("\n  ── Most wins by team (all seasons) ──")
    enriched.groupBy("winner") \
            .agg(F.sum("wins").alias("total_wins")) \
            .orderBy(F.desc("total_wins")) \
            .show(truncate=False)
    return count


# ── 4. Match summary ──────────────────────────────────────────────────────────

def build_match_summary(spark):
    logger.info("Building match summary …")
    matches  = spark.read.format("delta").load(SILVER_MATCHES_PATH)
    deliveries = spark.read.format("delta").load(SILVER_DELIVERIES_PATH)

    # total runs per match per inning
    inning_runs = (
        deliveries
        .groupBy("match_id", "inning")
        .agg(F.sum("total_runs").alias("inning_total_runs"))
    )

    # join to get full match picture
    summary = (
        matches
        .join(
            inning_runs.filter(F.col("inning") == 1)
                       .withColumnRenamed("inning_total_runs", "innings1_runs"),
            matches["id"] == inning_runs["match_id"], "left"
        )
        .drop("match_id", "inning")
        .withColumn("gold_created_at", F.current_timestamp())
        .select(
            "id", "season", "match_date", "venue", "city",
            "team1", "team2", "toss_winner", "toss_decision",
            "winner", "result", "is_playoff",
            "toss_winner_won", "innings1_runs",
            "player_of_match", "gold_created_at"
        )
    )

    count = summary.count()
    logger.info("  Match summary rows: %d", count)
    ensure_dirs(GOLD_MATCH_SUMMARY_PATH)
    summary.write.format("delta").mode("overwrite").save(GOLD_MATCH_SUMMARY_PATH)
    logger.info("  Saved → %s", GOLD_MATCH_SUMMARY_PATH)
    return count


# ── Runner ────────────────────────────────────────────────────────────────────

def run_gold() -> None:
    spark = get_spark("Gold-Analytics")

    b = build_batsman_stats(spark)
    w = build_bowler_stats(spark)
    t = build_team_season_wins(spark)
    m = build_match_summary(spark)

    logger.info("Gold layer complete.")
    logger.info("  Batsman stats : %d players", b)
    logger.info("  Bowler stats  : %d bowlers", w)
    logger.info("  Team wins     : %d rows", t)
    logger.info("  Match summary : %d matches", m)


if __name__ == "__main__":
    run_gold()
