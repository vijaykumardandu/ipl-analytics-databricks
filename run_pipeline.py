"""
run_pipeline.py
---------------
Master script — runs the full IPL analytics pipeline:

  Step 1  generate_data     → creates matches.csv + deliveries.csv
  Step 2  bronze_ingest     → raw Delta tables
  Step 3  silver_transform  → cleaned + enriched Delta tables
  Step 4  gold_analytics    → player rankings + team performance tables
  Step 5  analytics_queries → 6 SQL-style analytical results printed to console

Usage:
    python run_pipeline.py              # full run
    python run_pipeline.py --skip-gen  # skip data generation
    python run_pipeline.py --only gold # run one stage only
"""
import sys
import time
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")
DIVIDER = "─" * 58


def step(name, fn):
    logger.info(DIVIDER)
    logger.info("▶  %s", name)
    logger.info(DIVIDER)
    t0 = time.time()
    fn()
    logger.info("✓  Done in %.1fs\n", time.time() - t0)


def main():
    parser = argparse.ArgumentParser(description="IPL Analytics Pipeline")
    parser.add_argument("--skip-gen", action="store_true",
                        help="Skip data generation (use existing CSVs)")
    parser.add_argument("--only",
                        choices=["generate", "bronze", "silver", "gold", "queries"],
                        help="Run only one specific stage")
    args = parser.parse_args()

    from src.generate_data     import main as generate_data
    from src.bronze_ingest     import run_bronze
    from src.silver_transform  import run_silver
    from src.gold_analytics    import run_gold
    from src.analytics_queries import run_queries

    t_start = time.time()

    if args.only:
        stage_map = {
            "generate": ("Generate synthetic IPL data", generate_data),
            "bronze":   ("Bronze — raw ingestion",       run_bronze),
            "silver":   ("Silver — transform & enrich",  run_silver),
            "gold":     ("Gold — analytics tables",      run_gold),
            "queries":  ("Analytical queries",           run_queries),
        }
        name, fn = stage_map[args.only]
        step(name, fn)
    else:
        if not args.skip_gen:
            step("Step 1 — Generate synthetic IPL data", generate_data)
        else:
            logger.info("Skipping data generation (--skip-gen)")

        step("Step 2 — Bronze layer (raw ingestion)",       run_bronze)
        step("Step 3 — Silver layer (cleanse & enrich)",    run_silver)
        step("Step 4 — Gold layer (player & team analytics)", run_gold)
        step("Step 5 — Analytical queries",                 run_queries)

    logger.info(DIVIDER)
    logger.info("Pipeline complete in %.1fs", time.time() - t_start)
    logger.info(DIVIDER)


if __name__ == "__main__":
    main()
