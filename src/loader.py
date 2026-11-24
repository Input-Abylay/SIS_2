"""Stage 5 - store the cleaned records in SQLite3 (data/output.db).

Duplicate prevention / idempotency:
  * event_id is the PRIMARY KEY
  * rows are written with an UPSERT:
        INSERT ... ON CONFLICT(event_id) DO UPDATE ... WHERE excluded.updated_time > earthquakes.updated_time
    -> an already stored event is only changed if USGS published a NEWER version,
       so loading the same dataset twice changes nothing.

Run alone:  python -m src.loader          (loads data/cleaned_data.csv twice + verifies)
"""
import hashlib
import logging
import sqlite3

import pandas as pd

from src import config
from src.cleaner import FINAL_COLUMNS

log = logging.getLogger(__name__)


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(config.DB_PATH)
    connection.executescript((config.SQL_DIR / "schema.sql").read_text())
    return connection


def prepare_rows(df: pd.DataFrame) -> list[tuple]:
    df = df[FINAL_COLUMNS].copy()
    for column in ["event_time", "updated_time"]:
        df[column] = pd.to_datetime(df[column], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    df["tsunami_flag"] = df["tsunami_flag"].astype(bool).astype(int)
    df = df.astype(object).where(df.notna(), None)          # NaN -> SQL NULL
    return [tuple(row) for row in df.itertuples(index=False)]


def build_upsert_sql() -> str:
    columns = ", ".join(FINAL_COLUMNS)
    placeholders = ", ".join("?" for _ in FINAL_COLUMNS)
    updates = ", ".join(f"{c} = excluded.{c}" for c in FINAL_COLUMNS if c != "event_id")
    return (f"INSERT INTO earthquakes ({columns}) VALUES ({placeholders}) "
            f"ON CONFLICT(event_id) DO UPDATE SET {updates} "
            f"WHERE excluded.updated_time > earthquakes.updated_time")


def table_fingerprint(connection: sqlite3.Connection) -> str:
    """SHA-256 of the whole table content - identical hash = identical data."""
    digest = hashlib.sha256()
    for row in connection.execute("SELECT * FROM earthquakes ORDER BY event_id"):
        digest.update(repr(row).encode())
    return digest.hexdigest()


def count_rows(connection: sqlite3.Connection) -> int:
    return connection.execute("SELECT COUNT(*) FROM earthquakes").fetchone()[0]


def load_records(df: pd.DataFrame) -> dict:
    """One loading run. Returns counts before/after so repeated runs can be compared."""
    rows = prepare_rows(df)
    connection = get_connection()
    try:
        before = count_rows(connection)
        with connection:                                   # one transaction, rollback on error
            changes_before = connection.total_changes
            connection.executemany(build_upsert_sql(), rows)
            rows_written = connection.total_changes - changes_before
        after = count_rows(connection)
        fingerprint = table_fingerprint(connection)
    except sqlite3.Error as error:
        log.error("SQLite error while loading: %s", error)
        raise
    finally:
        connection.close()

    stats = {"processed_records": len(rows), "rows_before": before, "rows_after": after,
             "new_rows": after - before, "rows_inserted_or_updated": rows_written,
             "fingerprint": fingerprint}
    log.info("Load: %s", stats)
    return stats


def run_idempotency_test(df: pd.DataFrame) -> dict:
    """Execute the load stage twice with the same dataset and compare."""
    first = load_records(df)
    second = load_records(df)
    result = {
        "first_load": first,
        "second_load": second,
        "row_count_unchanged": first["rows_after"] == second["rows_after"],
        "second_run_new_rows": second["new_rows"],
        "second_run_rows_written": second["rows_inserted_or_updated"],
        "content_unchanged": first["fingerprint"] == second["fingerprint"],
    }
    result["idempotent"] = (result["row_count_unchanged"] and result["content_unchanged"]
                            and second["rows_inserted_or_updated"] == 0)
    return result


def parse_queries() -> list[tuple[str, str]]:
    """Read sql/verification_queries.sql -> [(name, sql), ...]."""
    queries = []
    for block in (config.SQL_DIR / "verification_queries.sql").read_text().split("-- name:")[1:]:
        name, sql = block.split("\n", 1)
        queries.append((name.split("(")[0].strip(), sql.strip()))
    return queries


def run_verification_queries(print_results: bool = True) -> dict:
    connection = sqlite3.connect(config.DB_PATH)
    results = {}
    try:
        for name, sql in parse_queries():
            table = pd.read_sql_query(sql, connection)
            results[name] = table
            if print_results:
                print(f"\n-- {name}\n{table.to_string(index=False) if len(table) else '(no rows)'}")
    finally:
        connection.close()
    return results


if __name__ == "__main__":
    config.setup_logging()
    cleaned = pd.read_csv(config.CLEANED_PATH)
    test = run_idempotency_test(cleaned)
    print(f"\nFirst load : {test['first_load']['rows_before']} -> {test['first_load']['rows_after']} rows")
    print(f"Second load: {test['second_load']['rows_before']} -> {test['second_load']['rows_after']} rows, "
          f"{test['second_run_rows_written']} rows written")
    print(f"Idempotent : {test['idempotent']}")
    run_verification_queries()
