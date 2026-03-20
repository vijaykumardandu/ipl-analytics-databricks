"""
generate_data.py
----------------
Generates synthetic IPL-like cricket data so the project runs completely
offline — no Kaggle account or download needed.

Produces two CSV files that mirror the real IPL dataset schema:
  data/raw/matches.csv      (~500 matches)
  data/raw/deliveries.csv   (~60,000 delivery records)

Run:
    python src/generate_data.py
"""
import os
import sys
import random
import logging
from datetime import date, timedelta

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import RAW_DIR, MATCHES_PATH, DELIVERIES_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger("generate_data")

random.seed(42)

# ── Constants ─────────────────────────────────────────────────────────────────
TEAMS = [
    "Mumbai Indians", "Chennai Super Kings", "Royal Challengers Bangalore",
    "Kolkata Knight Riders", "Sunrisers Hyderabad", "Delhi Capitals",
    "Punjab Kings", "Rajasthan Royals",
]

VENUES = [
    "Wankhede Stadium", "M. Chinnaswamy Stadium", "Eden Gardens",
    "Arun Jaitley Stadium", "Rajiv Gandhi Stadium", "MA Chidambaram Stadium",
    "Sawai Mansingh Stadium", "Punjab Cricket Association Stadium",
]

CITIES = [
    "Mumbai", "Bangalore", "Kolkata", "Delhi",
    "Hyderabad", "Chennai", "Jaipur", "Chandigarh",
]

PLAYERS = {
    "Mumbai Indians":                  ["Rohit Sharma",    "Ishan Kishan",    "Suryakumar Yadav", "Hardik Pandya",  "Jasprit Bumrah",   "Kieron Pollard", "Tim David",      "Tilak Varma"],
    "Chennai Super Kings":             ["MS Dhoni",        "Ruturaj Gaikwad", "Ambati Rayudu",    "Ravindra Jadeja","Deepak Chahar",    "Devon Conway",   "Moeen Ali",      "Shivam Dube"],
    "Royal Challengers Bangalore":     ["Virat Kohli",     "Faf du Plessis",  "Glenn Maxwell",    "Mohammed Siraj", "Harshal Patel",    "Dinesh Karthik", "Shahbaz Ahmed",  "Wanindu Hasaranga"],
    "Kolkata Knight Riders":           ["Shreyas Iyer",    "Andre Russell",   "Sunil Narine",     "Pat Cummins",    "Varun Chakravarthy","Nitish Rana",   "Sam Billings",   "Rinku Singh"],
    "Sunrisers Hyderabad":             ["Kane Williamson", "Abhishek Sharma", "Rahul Tripathi",   "T Natarajan",    "Bhuvneshwar Kumar","Washington Sundar","Marco Jansen", "Harry Brook"],
    "Delhi Capitals":                  ["David Warner",    "Prithvi Shaw",    "Rishabh Pant",     "Axar Patel",     "Anrich Nortje",    "Mitchell Marsh", "Kuldeep Yadav",  "Rovman Powell"],
    "Punjab Kings":                    ["Shikhar Dhawan",  "Mayank Agarwal",  "Liam Livingstone", "Sam Curran",     "Kagiso Rabada",    "Jonny Bairstow", "Arshdeep Singh", "Bhanuka Rajapaksa"],
    "Rajasthan Royals":                ["Sanju Samson",    "Jos Buttler",     "Yashasvi Jaiswal", "Ravichandran Ashwin","Trent Boult",  "Shimron Hetmyer","Devdutt Padikkal","Prasidh Krishna"],
}

SEASONS      = list(range(2008, 2024))
TOSS_OPTIONS = ["bat", "field"]
RESULTS      = ["normal", "normal", "normal", "tie", "no result"]
MATCH_TYPES  = ["League", "League", "League", "League", "Qualifier", "Eliminator", "Final"]


# ── Match generation ──────────────────────────────────────────────────────────

def generate_matches(n_per_season: int = 30) -> pd.DataFrame:
    rows = []
    match_id = 1

    for season in SEASONS:
        season_start = date(season, 4, 1)

        for match_num in range(n_per_season):
            teams_sample = random.sample(TEAMS, 2)
            team1, team2 = teams_sample[0], teams_sample[1]
            toss_winner  = random.choice([team1, team2])
            toss_decision= random.choice(TOSS_OPTIONS)
            winner       = random.choice([team1, team2, None])  # None = no result
            result       = random.choice(RESULTS)

            if result == "no result":
                winner = None
                win_by_runs = 0
                win_by_wickets = 0
            elif winner == team1:
                win_by_runs    = random.randint(1, 100)
                win_by_wickets = 0
            else:
                win_by_runs    = 0
                win_by_wickets = random.randint(1, 10)

            venue_idx = random.randint(0, len(VENUES) - 1)
            match_date = season_start + timedelta(days=match_num * 3)
            match_type = "Final" if match_num == n_per_season - 1 else \
                         "Qualifier" if match_num >= n_per_season - 3 else "League"

            rows.append({
                "id":              match_id,
                "season":          season,
                "city":            CITIES[venue_idx],
                "date":            match_date.strftime("%Y-%m-%d"),
                "match_type":      match_type,
                "player_of_match": random.choice(PLAYERS[team1] + PLAYERS[team2]),
                "venue":           VENUES[venue_idx],
                "team1":           team1,
                "team2":           team2,
                "toss_winner":     toss_winner,
                "toss_decision":   toss_decision,
                "winner":          winner if winner else "",
                "result":          result,
                "result_margin":   win_by_runs or win_by_wickets,
                "target_runs":     random.randint(120, 220),
                "target_overs":    20,
                "super_over":      "Y" if result == "tie" else "N",
                "method":          "D/L" if result == "no result" else "",
                "umpire1":         f"Umpire_{random.randint(1, 20)}",
                "umpire2":         f"Umpire_{random.randint(1, 20)}",
            })
            match_id += 1

    df = pd.DataFrame(rows)
    logger.info("Generated %d matches across %d seasons", len(df), len(SEASONS))
    return df


# ── Delivery generation ───────────────────────────────────────────────────────

def generate_deliveries(matches_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, match in matches_df.iterrows():
        match_id = match["id"]
        team1    = match["team1"]
        team2    = match["team2"]

        # Each team bats one innings
        for inning, (batting_team, bowling_team) in enumerate(
            [(team1, team2), (team2, team1)], start=1
        ):
            batsmen = PLAYERS[batting_team][:6]
            bowlers = PLAYERS[bowling_team][4:8]

            over_count = random.randint(15, 20)  # not always 20 overs

            for over in range(1, over_count + 1):
                bowler      = random.choice(bowlers)
                balls_in_over = 6
                ball_num    = 0

                while ball_num < balls_in_over:
                    ball_num += 1
                    batsman      = random.choice(batsmen)
                    non_striker  = random.choice([b for b in batsmen if b != batsman])

                    batsman_runs = random.choices(
                        [0, 1, 2, 3, 4, 6],
                        weights=[35, 30, 10, 2, 15, 8],
                    )[0]

                    # extras
                    extra_type   = random.choices(
                        ["", "wides", "noballs", "byes", "legbyes"],
                        weights=[80, 8, 5, 4, 3],
                    )[0]
                    extra_runs   = 1 if extra_type in ("wides", "noballs") else 0

                    # wicket
                    is_wicket    = random.random() < 0.04
                    dismissed    = batsman if is_wicket else None
                    dismissal    = random.choice([
                        "caught", "bowled", "lbw", "run out", "stumped"
                    ]) if is_wicket else None

                    # wides / noballs add an extra delivery
                    if extra_type in ("wides", "noballs"):
                        balls_in_over += 1

                    rows.append({
                        "match_id":         match_id,
                        "inning":           inning,
                        "batting_team":     batting_team,
                        "bowling_team":     bowling_team,
                        "over":             over,
                        "ball":             ball_num,
                        "batsman":          batsman,
                        "non_striker":      non_striker,
                        "bowler":           bowler,
                        "is_super_over":    0,
                        "wide_runs":        extra_runs if extra_type == "wides"   else 0,
                        "bye_runs":         extra_runs if extra_type == "byes"    else 0,
                        "legbye_runs":      extra_runs if extra_type == "legbyes" else 0,
                        "noball_runs":      extra_runs if extra_type == "noballs" else 0,
                        "penalty_runs":     0,
                        "batsman_runs":     batsman_runs if extra_type != "wides" else 0,
                        "extra_runs":       extra_runs,
                        "total_runs":       batsman_runs + extra_runs,
                        "player_dismissed": dismissed,
                        "dismissal_kind":   dismissal,
                        "fielder":          random.choice(PLAYERS[bowling_team]) if is_wicket else None,
                    })

    df = pd.DataFrame(rows)
    logger.info("Generated %d delivery records", len(df))
    return df


def main() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)

    logger.info("Generating matches …")
    matches_df = generate_matches(n_per_season=30)
    matches_df.to_csv(MATCHES_PATH, index=False)
    logger.info("  Saved → %s  (%d rows)", MATCHES_PATH, len(matches_df))

    logger.info("Generating deliveries …")
    deliveries_df = generate_deliveries(matches_df)
    deliveries_df.to_csv(DELIVERIES_PATH, index=False)
    logger.info("  Saved → %s  (%d rows)", DELIVERIES_PATH, len(deliveries_df))

    logger.info("Data generation complete.")
    logger.info("  Total deliveries: %d", len(deliveries_df))
    logger.info("  Total matches   : %d", len(matches_df))


if __name__ == "__main__":
    main()
