"""Stage 3 - validate the consumed Kafka records with Pandera.

Validation happens on the RAW consumed data, before cleaning. Invalid rows are
quarantined to data/invalid_records.csv together with their error messages;
only valid rows continue to the cleaning stage.

Because the real USGS data is usually already clean, four deliberately broken
TEST records (is_test_record = True, ids starting with "TEST_") are appended
to prove that every rule really works. They are always excluded from the
processed dataset.

Run alone:  python -m src.validator
"""
import json
import logging
from datetime import datetime, timezone

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

from src import config

log = logging.getLogger(__name__)

ALLOWED_STATUS = ["automatic", "reviewed"]
ALLOWED_EVENT_TYPES = ["earthquake", "explosion", "quarry blast", "landslide", "ice quake",
                       "mining explosion", "rock burst", "sonic boom", "other event",
                       "volcanic eruption", "chemical explosion", "nuclear explosion"]
ALLOWED_ALERTS = ["green", "yellow", "orange", "red"]

# Requested time window in epoch milliseconds (used by the date/time rule)
WINDOW_START_MS = int(pd.Timestamp(config.USGS_START_TIME, tz="UTC").timestamp() * 1000)
WINDOW_END_MS = int(pd.Timestamp(config.USGS_END_TIME, tz="UTC").timestamp() * 1000)

EARTHQUAKE_SCHEMA = pa.DataFrameSchema(
    columns={
        # R1 unique identifier: required, unique, USGS format (lowercase letters + digits)
        "id": pa.Column(str, nullable=False, unique=True, report_duplicates="exclude_first",
                        checks=pa.Check.str_matches(r"^[a-z0-9]+$")),
        # R2 magnitude: numeric type, required, physically plausible range
        "mag": pa.Column(float, nullable=False, coerce=True,
                         checks=pa.Check.in_range(-1.0, 10.0)),
        # R3 coordinates within the globe
        "latitude": pa.Column(float, nullable=False, coerce=True, checks=pa.Check.in_range(-90, 90)),
        "longitude": pa.Column(float, nullable=False, coerce=True, checks=pa.Check.in_range(-180, 180)),
        # R4 depth in km: slightly negative is allowed (events above sea level), max ~ 800 km
        "depth": pa.Column(float, nullable=False, coerce=True, checks=pa.Check.in_range(-10, 800)),
        # R5 date/time: event time must be inside the requested window
        "time": pa.Column("int64", nullable=False, coerce=True,
                          checks=pa.Check.in_range(WINDOW_START_MS, WINDOW_END_MS,
                                                   include_max=False)),
        "updated": pa.Column("int64", nullable=False, coerce=True),
        # R6 allowed categorical values
        "status": pa.Column(str, nullable=False, checks=pa.Check.isin(ALLOWED_STATUS)),
        "type": pa.Column(str, nullable=False, checks=pa.Check.isin(ALLOWED_EVENT_TYPES)),
        "alert": pa.Column(str, nullable=True, checks=pa.Check.isin(ALLOWED_ALERTS)),
        "tsunami": pa.Column("int64", nullable=False, coerce=True, checks=pa.Check.isin([0, 1])),
        # R7 other numeric boundaries
        "sig": pa.Column("int64", nullable=False, coerce=True, checks=pa.Check.ge(0)),
        "gap": pa.Column(float, nullable=True, coerce=True, checks=pa.Check.in_range(0, 360)),
        "rms": pa.Column(float, nullable=True, coerce=True, checks=pa.Check.ge(0)),
        "felt": pa.Column(float, nullable=True, coerce=True, checks=pa.Check.ge(0)),
        # R8 network code: 2 lowercase letters; magnitude type starts with "m" (ml, mb, mww...)
        "net": pa.Column(str, nullable=False, checks=pa.Check.str_matches(r"^[a-z]{2}$")),
        "magType": pa.Column(str, nullable=False, checks=pa.Check.str_matches(r"(?i)^m[a-z_]*$")),
        "place": pa.Column(str, nullable=True),
        "is_test_record": pa.Column(bool, nullable=False),
    },
    checks=[
        # R9 custom cross-column rule: a record cannot be updated before the event happened
        pa.Check(lambda df: df["updated"] >= df["time"],
                 name="updated_not_before_event_time",
                 error="updated must be >= time"),
    ],
    strict=False,   # extra raw columns (title, url, code, ...) are allowed
)


def build_test_records(real_records: list[dict]) -> list[dict]:
    """Four intentionally invalid records - each one breaks several rules."""
    template = dict(real_records[0])
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    tests = [
        {**template, "id": "TEST_001", "mag": 12.5, "latitude": 95.0, "longitude": -200.0},
        {**template, "id": "TEST_002", "status": "unknown", "type": "meteor strike",
         "tsunami": 2, "net": "USGS", "alert": "purple"},
        {**template, "id": "TEST_003", "mag": "not-a-number", "depth": 1200.0,
         "time": now_ms + 10 ** 10, "updated": now_ms, "sig": -5},
        # same id as a real event -> violates the uniqueness rule
        {**template, "id": real_records[0]["id"], "mag": None, "gap": 400.0},
    ]
    for test in tests:
        test["is_test_record"] = True
        test["place"] = "TEST RECORD - must be rejected"
    return tests


def describe_failure(row: pd.Series) -> str:
    if row["schema_context"] == "DataFrameSchema":       # cross-column rule -> one message per row
        return f"row: {row['check']}"
    return f"{row['column']}: {row['check']} (got {row['failure_case']})"


def find_failures(frame: pd.DataFrame, max_rounds: int = 5) -> pd.DataFrame:
    """Validate lazily and return one row per (record index, error).

    A value with a wrong type (e.g. mag = "not-a-number") makes the whole column
    un-coercible, so Pandera cannot run that column's other checks in the same
    pass. We therefore quarantine the rows found so far and re-validate the
    remaining rows until the schema passes.
    """
    collected = []
    remaining = frame
    for _ in range(max_rounds):
        try:
            EARTHQUAKE_SCHEMA.validate(remaining, lazy=True)   # lazy -> collect ALL errors
            break
        except SchemaErrors as errors:
            cases = errors.failure_cases
            row_cases = cases[cases["index"].notna()].copy()
            if row_cases.empty:
                # column-level problem (e.g. a whole column missing) - cannot be fixed row by row
                raise RuntimeError(f"Schema-level validation failure:\n{cases}") from errors
            row_cases["index"] = row_cases["index"].astype(int)
            row_cases["error"] = row_cases.apply(describe_failure, axis=1)
            collected.append(row_cases)
            remaining = remaining.drop(index=row_cases["index"].unique())
    else:
        raise RuntimeError("Validation did not converge")

    if not collected:
        return pd.DataFrame(columns=["index", "column", "check", "failure_case", "error"])
    return (pd.concat(collected)
              .drop_duplicates(subset=["index", "error"])
              [["index", "column", "check", "failure_case", "error"]])


def validate_records(records: list[dict], add_test_records: bool = True) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    real_records = [{**r, "is_test_record": False} for r in records]
    test_records = build_test_records(records) if add_test_records else []
    frame = pd.DataFrame(real_records + test_records).reset_index(drop=True)

    failure_cases = find_failures(frame)
    errors_per_row = failure_cases.groupby("index")["error"].apply(lambda e: " | ".join(sorted(set(e))))

    invalid_mask = frame.index.isin(errors_per_row.index)
    # Handling strategy: quarantine invalid rows (kept for inspection), continue with valid rows.
    invalid_df = frame[invalid_mask].copy()
    invalid_df["validation_errors"] = errors_per_row
    valid_df = frame[~invalid_mask & ~frame["is_test_record"]].copy()   # test rows never pass on

    invalid_df.to_csv(config.INVALID_PATH, index=False)
    valid_df.to_csv(config.VALID_PATH, index=False)
    failure_cases.to_csv(config.RESULTS_DIR / "validation_failure_cases.csv", index=False)

    stats = {
        "records_checked": len(frame),
        "real_records": len(real_records),
        "test_records": len(test_records),
        "valid_records": len(valid_df),
        "invalid_records": int(invalid_mask.sum()),
        "invalid_real_records": int((invalid_mask & ~frame["is_test_record"]).sum()),
        "invalid_test_records": int((invalid_mask & frame["is_test_record"]).sum()),
        "failures_by_check": failure_cases.groupby(["column", "check"], dropna=False).size()
                                          .rename("count").reset_index().astype(str)
                                          .to_dict(orient="records"),
    }
    log.info("Validation: %d checked, %d valid, %d invalid (%d test)",
             stats["records_checked"], stats["valid_records"], stats["invalid_records"],
             stats["invalid_test_records"])
    return valid_df, invalid_df, stats


def print_report(invalid_df: pd.DataFrame, stats: dict) -> None:
    print(f"\nRecords checked : {stats['records_checked']} "
          f"({stats['real_records']} real + {stats['test_records']} test)")
    print(f"Valid records   : {stats['valid_records']}")
    print(f"Invalid records : {stats['invalid_records']} "
          f"(real: {stats['invalid_real_records']}, test: {stats['invalid_test_records']})")
    print("\nInvalid records and their errors:")
    for _, row in invalid_df.iterrows():
        print(f"  - {row['id']} (test={row['is_test_record']}):")
        for error in row["validation_errors"].split(" | "):
            print(f"      * {error}")


if __name__ == "__main__":
    config.setup_logging()
    consumed = json.loads(config.CONSUMED_PATH.read_text())
    _, invalid, result = validate_records(consumed)
    print_report(invalid, result)
