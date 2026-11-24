"""Run the complete SIS2 pipeline:

    USGS API -> Kafka Producer -> Kafka topic -> Kafka Consumer -> Pandera validation
             -> pandas/NumPy cleaning -> SQLite3 (loaded twice) -> EDA + figures

Usage:
    python main.py                  # full pipeline (calls the API)
    python main.py --skip-collect   # reuse data/raw_data.json (no internet needed)

Kafka must be running first (see README.md).
"""
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from kafka.errors import KafkaError, NoBrokersAvailable

from src import config
from src.analysis import run_analysis
from src.api_client import collect_earthquakes, load_raw_records
from src.cleaner import clean_records
from src.consumer import consume_records
from src.loader import run_idempotency_test, run_verification_queries
from src.producer import publish_records
from src.validator import print_report, validate_records

log = logging.getLogger("pipeline")


def banner(title: str) -> None:
    print(f"\n{'=' * 70}\n  {title}\n{'=' * 70}", flush=True)


def run_pipeline(skip_collect: bool) -> dict:
    started = time.monotonic()

    banner("STAGE 1  Data collection (USGS API)")
    records = load_raw_records() if skip_collect else collect_earthquakes()
    print(f"Collected API records: {len(records)}"
          + ("  (re-used data/raw_data.json)" if skip_collect else ""))

    banner("STAGE 2  Kafka streaming (producer -> topic -> consumer)")
    producer_stats = publish_records(records)
    print(f"Published to topic '{config.KAFKA_TOPIC}': {producer_stats['published']} "
          f"(failed: {producer_stats['failed']}, run_id={producer_stats['run_id']})")
    consumed, consumer_stats = consume_records(run_id=producer_stats["run_id"])
    print(f"Consumed from topic: {consumer_stats['consumed']} "
          f"(corrupted: {consumer_stats['corrupted']}, "
          f"skipped from older runs: {consumer_stats['skipped_other_runs']})")

    banner("STAGE 3  Pandera validation")
    valid_df, invalid_df, validation_stats = validate_records(consumed)
    print_report(invalid_df, validation_stats)

    banner("STAGE 4  Cleaning and transformation (pandas + NumPy)")
    cleaned_df, cleaning_stats = clean_records(valid_df)
    print(f"Duplicates removed: {cleaning_stats['rows_before_dedup'] - cleaning_stats['rows_after_dedup']}")
    print(f"Missing values before: {cleaning_stats['missing_before']}")
    print(f"Missing values after : {cleaning_stats['missing_after']}")
    print(f"Records after cleaning: {cleaning_stats['rows_out']}")

    banner("STAGE 5  SQLite3 load (executed twice -> idempotency test)")
    load_stats = run_idempotency_test(cleaned_df)
    first, second = load_stats["first_load"], load_stats["second_load"]
    print(f"Load #1: rows {first['rows_before']} -> {first['rows_after']} "
          f"({first['rows_inserted_or_updated']} written)")
    print(f"Load #2: rows {second['rows_before']} -> {second['rows_after']} "
          f"({second['rows_inserted_or_updated']} written)")
    print(f"Table fingerprint identical: {load_stats['content_unchanged']}  ->  "
          f"idempotent = {load_stats['idempotent']}")
    query_results = run_verification_queries()

    banner("STAGE 6  Exploratory data analysis")
    analysis = run_analysis()
    print(f"Figures saved: {', '.join(analysis['figures'])}")
    print("Text summary : results/eda_summary.txt")

    stored = int(query_results["total_rows"].iloc[0, 0])
    counts = {
        "collected_api_records": len(records),
        "published_to_kafka": producer_stats["published"],
        "consumed_from_kafka": consumer_stats["consumed"],
        "test_records_added": validation_stats["test_records"],
        "validated_total": validation_stats["records_checked"],
        "valid_records": validation_stats["valid_records"],
        "invalid_records": validation_stats["invalid_records"],
        "records_after_cleaning": cleaning_stats["rows_out"],
        "stored_in_sqlite": stored,
    }
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "api_window": [config.USGS_START_TIME, config.USGS_END_TIME, config.USGS_MIN_MAGNITUDE],
        "counts": counts,
        "producer": producer_stats,
        "consumer": consumer_stats,
        "validation": validation_stats,
        "cleaning": cleaning_stats,
        "loading": load_stats,
        "analysis_facts": analysis["facts"],
        "checks": {
            "published_equals_collected": counts["published_to_kafka"] == counts["collected_api_records"],
            "consumed_equals_published": counts["consumed_from_kafka"] == counts["published_to_kafka"],
            "stored_equals_cleaned": counts["stored_in_sqlite"] == counts["records_after_cleaning"],
            "no_test_records_in_db": int(query_results["no_test_records"].iloc[0, 0]) == 0,
            "no_duplicate_ids_in_db": query_results["duplicate_ids"].empty,
            "idempotent_reload": load_stats["idempotent"],
        },
        "runtime_seconds": round(time.monotonic() - started, 1),
    }
    config.SUMMARY_PATH.write_text(json.dumps(summary, indent=2, default=str))
    return summary


def print_verification(summary: dict) -> None:
    banner("PIPELINE VERIFICATION - record counts")
    counts = summary["counts"]
    table = pd.DataFrame({"stage": list(counts), "records": list(counts.values())})
    print(table.to_string(index=False))
    print("\nExplanation of differences:")
    print(f"  * validated_total = consumed + {counts['test_records_added']} deliberately invalid test records")
    print(f"  * invalid = {summary['validation']['invalid_real_records']} real + "
          f"{summary['validation']['invalid_test_records']} test records (quarantined in data/invalid_records.csv)")
    print(f"  * cleaning removed {counts['valid_records'] - counts['records_after_cleaning']} duplicate event ids")
    print("\nChecks:")
    for name, passed in summary["checks"].items():
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}")
    print(f"\nSummary written to {config.SUMMARY_PATH.relative_to(config.PROJECT_ROOT)} "
          f"({summary['runtime_seconds']} s)")


if __name__ == "__main__":
    config.setup_logging()
    parser = argparse.ArgumentParser(description="USGS earthquake streaming pipeline")
    parser.add_argument("--skip-collect", action="store_true",
                        help="re-use data/raw_data.json instead of calling the API")
    args = parser.parse_args()
    try:
        result = run_pipeline(args.skip_collect)
    except NoBrokersAvailable:
        print(f"\nERROR: Kafka broker not reachable at {config.KAFKA_BOOTSTRAP_SERVERS}. "
              "Start Kafka first (see README.md).")
        sys.exit(1)
    except (KafkaError, RuntimeError, FileNotFoundError) as error:
        log.exception("Pipeline failed: %s", error)
        sys.exit(1)
    print_verification(result)
    sys.exit(0 if all(result["checks"].values()) else 2)
