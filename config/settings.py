import os

# ── Base paths ────────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR      = os.path.join(BASE_DIR, "data")
RAW_DIR       = os.path.join(DATA_DIR, "raw")
DELTA_DIR     = os.path.join(DATA_DIR, "delta")

# ── Source files ──────────────────────────────────────────────────────────────
MATCHES_PATH    = os.path.join(RAW_DIR, "matches.csv")
DELIVERIES_PATH = os.path.join(RAW_DIR, "deliveries.csv")

# ── Delta table paths ─────────────────────────────────────────────────────────
BRONZE_MATCHES_PATH     = os.path.join(DELTA_DIR, "bronze", "matches")
BRONZE_DELIVERIES_PATH  = os.path.join(DELTA_DIR, "bronze", "deliveries")

SILVER_MATCHES_PATH     = os.path.join(DELTA_DIR, "silver", "matches")
SILVER_DELIVERIES_PATH  = os.path.join(DELTA_DIR, "silver", "deliveries")

GOLD_BATSMAN_PATH       = os.path.join(DELTA_DIR, "gold", "batsman_stats")
GOLD_BOWLER_PATH        = os.path.join(DELTA_DIR, "gold", "bowler_stats")
GOLD_TEAM_PATH          = os.path.join(DELTA_DIR, "gold", "team_season_wins")
GOLD_MATCH_SUMMARY_PATH = os.path.join(DELTA_DIR, "gold", "match_summary")

# ── Spark settings ────────────────────────────────────────────────────────────
SPARK_APP_NAME          = "IPL-Analytics"
SPARK_MASTER            = "local[*]"

# ── Quality thresholds ────────────────────────────────────────────────────────
MIN_MATCHES_BATSMAN     = 10   # minimum matches to include a batsman in rankings
MIN_MATCHES_BOWLER      = 10   # minimum matches to include a bowler in rankings
MIN_WICKETS_BOWLER      = 10   # minimum wickets for economy leaderboard
