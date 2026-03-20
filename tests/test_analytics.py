"""
test_analytics.py
-----------------
Unit tests for Silver and Gold layer transformations.

Run:
    pytest tests/ -v
"""
import os
import sys
import pytest
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, BooleanType
)
from src.utils import get_spark


@pytest.fixture(scope="session")
def spark():
    return get_spark("TestSuite")


@pytest.fixture
def sample_deliveries(spark):
    data = [
        Row(match_id=1, inning=1, batting_team="MI", bowling_team="CSK",
            over=1, ball=1, batsman="Rohit Sharma", non_striker="Ishan Kishan",
            bowler="Deepak Chahar", is_super_over=0,
            wide_runs=0, bye_runs=0, legbye_runs=0, noball_runs=0,
            batsman_runs=4, extra_runs=0, total_runs=4,
            player_dismissed=None, dismissal_kind=None, fielder=None),

        Row(match_id=1, inning=1, batting_team="MI", bowling_team="CSK",
            over=1, ball=2, batsman="Rohit Sharma", non_striker="Ishan Kishan",
            bowler="Deepak Chahar", is_super_over=0,
            wide_runs=0, bye_runs=0, legbye_runs=0, noball_runs=0,
            batsman_runs=6, extra_runs=0, total_runs=6,
            player_dismissed=None, dismissal_kind=None, fielder=None),

        Row(match_id=1, inning=1, batting_team="MI", bowling_team="CSK",
            over=1, ball=3, batsman="Rohit Sharma", non_striker="Ishan Kishan",
            bowler="Deepak Chahar", is_super_over=0,
            wide_runs=0, bye_runs=0, legbye_runs=0, noball_runs=0,
            batsman_runs=0, extra_runs=0, total_runs=0,
            player_dismissed=None, dismissal_kind=None, fielder=None),

        Row(match_id=1, inning=1, batting_team="MI", bowling_team="CSK",
            over=1, ball=4, batsman="Ishan Kishan", non_striker="Rohit Sharma",
            bowler="Deepak Chahar", is_super_over=0,
            wide_runs=0, bye_runs=0, legbye_runs=0, noball_runs=0,
            batsman_runs=1, extra_runs=0, total_runs=1,
            player_dismissed="Ishan Kishan", dismissal_kind="caught", fielder="MS Dhoni"),
    ]

    schema = StructType([
        StructField("match_id",         IntegerType(), True),
        StructField("inning",           IntegerType(), True),
        StructField("batting_team",     StringType(),  True),
        StructField("bowling_team",     StringType(),  True),
        StructField("over",             IntegerType(), True),
        StructField("ball",             IntegerType(), True),
        StructField("batsman",          StringType(),  True),
        StructField("non_striker",      StringType(),  True),
        StructField("bowler",           StringType(),  True),
        StructField("is_super_over",    IntegerType(), True),
        StructField("wide_runs",        IntegerType(), True),
        StructField("bye_runs",         IntegerType(), True),
        StructField("legbye_runs",      IntegerType(), True),
        StructField("noball_runs",      IntegerType(), True),
        StructField("batsman_runs",     IntegerType(), True),
        StructField("extra_runs",       IntegerType(), True),
        StructField("total_runs",       IntegerType(), True),
        StructField("player_dismissed", StringType(),  True),
        StructField("dismissal_kind",   StringType(),  True),
        StructField("fielder",          StringType(),  True),
    ])
    return spark.createDataFrame(data, schema=schema)


# ── Silver transform tests ────────────────────────────────────────────────────

class TestSilverDeliveries:

    def _enrich(self, df):
        return (
            df
            .withColumn("is_boundary",
                F.col("batsman_runs").isin([4, 6]).cast("boolean"))
            .withColumn("is_six",
                (F.col("batsman_runs") == 6).cast("boolean"))
            .withColumn("is_four",
                (F.col("batsman_runs") == 4).cast("boolean"))
            .withColumn("is_dot_ball",
                ((F.col("batsman_runs") == 0) &
                 (F.col("extra_runs") == 0)).cast("boolean"))
            .withColumn("is_wicket_delivery",
                F.col("player_dismissed").isNotNull().cast("boolean"))
            .withColumn("is_wide",
                (F.col("wide_runs") > 0).cast("boolean"))
        )

    def test_is_boundary_flagged(self, sample_deliveries):
        df = self._enrich(sample_deliveries)
        boundaries = df.filter(F.col("is_boundary") == True).count()
        assert boundaries == 2  # 4 and 6

    def test_is_six_correct(self, sample_deliveries):
        df = self._enrich(sample_deliveries)
        sixes = df.filter(F.col("is_six") == True).count()
        assert sixes == 1

    def test_is_four_correct(self, sample_deliveries):
        df = self._enrich(sample_deliveries)
        fours = df.filter(F.col("is_four") == True).count()
        assert fours == 1

    def test_is_dot_ball_correct(self, sample_deliveries):
        df = self._enrich(sample_deliveries)
        dots = df.filter(F.col("is_dot_ball") == True).count()
        assert dots == 1  # ball 3: 0 runs, 0 extras

    def test_is_wicket_delivery_correct(self, sample_deliveries):
        df = self._enrich(sample_deliveries)
        wickets = df.filter(F.col("is_wicket_delivery") == True).count()
        assert wickets == 1  # Ishan Kishan dismissed

    def test_no_wides_in_sample(self, sample_deliveries):
        df = self._enrich(sample_deliveries)
        wides = df.filter(F.col("is_wide") == True).count()
        assert wides == 0


# ── Gold analytics tests ──────────────────────────────────────────────────────

class TestGoldBatsmanStats:

    def test_strike_rate_calculation(self, spark):
        data = [Row(batsman="Rohit", total_runs=100, balls_faced=80)]
        df = spark.createDataFrame(data)
        result = df.withColumn(
            "strike_rate",
            F.round(F.col("total_runs") / F.col("balls_faced") * 100, 2)
        ).first()["strike_rate"]
        assert result == 125.0

    def test_batting_average_no_dismissals(self, spark):
        data = [Row(batsman="Virat", total_runs=50, times_dismissed=0)]
        df = spark.createDataFrame(data)
        result = df.withColumn(
            "batting_average",
            F.when(F.col("times_dismissed") == 0, F.col("total_runs"))
             .otherwise(F.col("total_runs") / F.col("times_dismissed"))
        ).first()["batting_average"]
        assert result == 50

    def test_performance_tier_elite(self, spark):
        data = [Row(overall_rank=1), Row(overall_rank=11), Row(overall_rank=35)]
        df = spark.createDataFrame(data)
        result = df.withColumn("tier",
            F.when(F.col("overall_rank") <= 10, "Elite")
             .when(F.col("overall_rank") <= 30, "Top Performer")
             .when(F.col("overall_rank") <= 60, "Regular")
             .otherwise("Occasional")
        ).collect()
        tiers = [r["tier"] for r in result]
        assert tiers == ["Elite", "Top Performer", "Regular"]


class TestGoldBowlerStats:

    def test_economy_rate_calculation(self, spark):
        data = [Row(bowler="Bumrah", runs_conceded=120, balls_bowled=60)]
        df = spark.createDataFrame(data)
        result = df.withColumn(
            "economy_rate",
            F.round(F.col("runs_conceded") / (F.col("balls_bowled") / 6), 2)
        ).first()["economy_rate"]
        assert result == 12.0

    def test_dot_ball_pct(self, spark):
        data = [Row(bowler="Narine", dot_balls=40, balls_bowled=60)]
        df = spark.createDataFrame(data)
        result = df.withColumn(
            "dot_ball_pct",
            F.round(F.col("dot_balls") / F.col("balls_bowled") * 100, 2)
        ).first()["dot_ball_pct"]
        assert result == 66.67
