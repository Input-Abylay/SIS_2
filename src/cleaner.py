"""Stage 4 - clean and transform the validated records with pandas + NumPy.

Steps (each one is explained in the report):
  1. duplicate detection/removal (Kafka is at-least-once -> same event can arrive twice)
  2. data type conversion (epoch ms -> UTC datetime, int/bool/category types)
  3. text standardization (lower-case codes, trimmed place, extracted region)
  4. missing-value handling (felt -> 0, alert -> "none", quality metrics -> median)
  5. NumPy derived features (energy, depth class, distance to Almaty, log-scale)
  6. multi-condition filtering, GroupBy aggregation, pivot_table / melt reshaping

Run alone:  python -m src.cleaner
"""
import logging

import numpy as np
import pandas as pd

from src import config

log = logging.getLogger(__name__)

ALMATY_LAT, ALMATY_LON = 43.2389, 76.8897
EARTH_RADIUS_KM = 6371.0

# Final columns stored in SQLite (order matters for the loader)
FINAL_COLUMNS = [
    "event_id", "event_time", "updated_time", "event_date", "event_hour", "day_of_week",
    "magnitude", "mag_type", "magnitude_class", "depth_km", "depth_class",
    "latitude", "longitude", "place", "region", "event_type", "status", "network",
    "alert_level", "tsunami_flag", "significance", "felt_reports", "station_count",
    "azimuthal_gap", "rms", "min_station_distance", "energy_joules", "log10_energy",
    "distance_to_almaty_km", "url",
]


def remove_duplicates(df: pd.DataFrame, report: dict) -> pd.DataFrame:
    report["rows_before_dedup"] = len(df)
    report["exact_duplicates"] = int(df.duplicated().sum())
    report["duplicate_event_ids"] = int(df.duplicated(subset="id").sum())
    # keep the most recently updated version of every event
    df = (df.sort_values("updated")
            .drop_duplicates(subset="id", keep="last")
            .sort_values("time")
            .reset_index(drop=True))
    report["rows_after_dedup"] = len(df)
    return df


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["event_time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    df["updated_time"] = pd.to_datetime(df["updated"], unit="ms", utc=True)
    for column in ["mag", "depth", "latitude", "longitude", "felt", "dmin", "rms", "gap", "nst"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["tsunami"] = df["tsunami"].astype(int).astype(bool)
    df["sig"] = df["sig"].astype(int)
    return df


def normalize_text(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for column in ["magType", "type", "status", "net", "alert"]:
        df[column] = df[column].astype("string").str.strip().str.lower()
    df["place"] = (df["place"].astype("string").str.strip()
                   .str.replace(r"\s+", " ", regex=True))
    # "18 km NE of Port-Olry, Vanuatu" -> "Vanuatu";  "south of the Fiji Islands" -> "South Of The Fiji Islands"
    region = df["place"].str.split(",").str[-1].str.strip()
    region = region.str.replace(r"^\d+\s*km\s+[NSEW]{1,3}\s+of\s+", "", regex=True)
    region = region.str.replace(r"\s+region$", "", case=False, regex=True)   # "Russia region" = "Russia"
    region = region.replace({"CA": "California", "AK": "Alaska", "NV": "Nevada", "HI": "Hawaii",
                             "TX": "Texas", "OK": "Oklahoma", "WA": "Washington", "UT": "Utah",
                             "MX": "Mexico", "B.C.": "Baja California", "PR": "Puerto Rico"})
    df["region"] = region.str.title().fillna("Unknown")
    return df


def handle_missing(df: pd.DataFrame, report: dict) -> pd.DataFrame:
    df = df.copy()
    columns = ["place", "felt", "cdi", "mmi", "alert", "nst", "dmin", "rms", "gap"]
    report["missing_before"] = {c: int(df[c].isna().sum()) for c in columns if c in df}

    df["felt"] = df["felt"].fillna(0)                 # no "Did you feel it?" reports = 0 reports
    df["alert"] = df["alert"].fillna("none")          # PAGER alert only issued for big events
    df["place"] = df["place"].fillna("Unknown location")
    # station-quality metrics: median of the same network, global median as fallback
    for column in ["dmin", "gap", "rms", "nst"]:
        df[column] = df[column].fillna(df.groupby("net")[column].transform("median"))
        df[column] = df[column].fillna(df[column].median())

    report["missing_after"] = {c: int(df[c].isna().sum()) for c in report["missing_before"]}
    return df


def add_numpy_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    magnitude = df["mag"].to_numpy()
    # Gutenberg-Richter energy relation: log10(E [J]) = 1.5 * M + 4.8
    df["log10_energy"] = np.round(1.5 * magnitude + 4.8, 3)
    df["energy_joules"] = np.power(10.0, df["log10_energy"].to_numpy())
    # depth classes used in seismology
    depth = df["depth"].to_numpy()
    df["depth_class"] = np.select([depth < 70, depth < 300], ["shallow", "intermediate"], "deep")
    df["magnitude_class"] = np.select(
        [magnitude < 4.0, magnitude < 5.0, magnitude < 6.0, magnitude < 7.0],
        ["minor", "light", "moderate", "strong"], "major")
    # great-circle (haversine) distance from Almaty, vectorised with NumPy
    lat1, lon1 = np.radians(ALMATY_LAT), np.radians(ALMATY_LON)
    lat2, lon2 = np.radians(df["latitude"].to_numpy()), np.radians(df["longitude"].to_numpy())
    a = (np.sin((lat2 - lat1) / 2) ** 2
         + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2)
    df["distance_to_almaty_km"] = np.round(2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a)), 1)
    return df


def finalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "id": "event_id", "mag": "magnitude", "magType": "mag_type", "depth": "depth_km",
        "type": "event_type", "net": "network", "alert": "alert_level", "tsunami": "tsunami_flag",
        "sig": "significance", "felt": "felt_reports", "nst": "station_count",
        "gap": "azimuthal_gap", "dmin": "min_station_distance",
    })
    df["event_date"] = df["event_time"].dt.date.astype(str)
    df["event_hour"] = df["event_time"].dt.hour
    df["day_of_week"] = df["event_time"].dt.day_name()
    df["felt_reports"] = df["felt_reports"].astype(int)
    df["station_count"] = df["station_count"].round().astype(int)
    return df[FINAL_COLUMNS]


def build_aggregations(df: pd.DataFrame) -> dict:
    """Filtering, GroupBy and reshaping on the cleaned data; tables saved to data/aggregations/."""
    tables = {}

    # Multi-condition filter: strong, shallow, reviewed earthquakes -> the most damaging type
    tables["filter_strong_shallow"] = df[(df["magnitude"] >= 5.0)
                                         & (df["depth_class"] == "shallow")
                                         & (df["status"] == "reviewed")
                                         & (df["event_type"] == "earthquake")]

    # GroupBy + several aggregation functions per region
    tables["groupby_region"] = (df.groupby("region")
                                  .agg(events=("event_id", "count"),
                                       mean_magnitude=("magnitude", "mean"),
                                       max_magnitude=("magnitude", "max"),
                                       median_depth_km=("depth_km", "median"),
                                       total_energy_joules=("energy_joules", "sum"))
                                  .round(3)
                                  .sort_values("events", ascending=False)
                                  .reset_index())
    tables["groupby_network"] = (df.groupby("network")["magnitude"]
                                   .agg(["count", "mean", "std", "min", "max"]).round(3)
                                   .sort_values("count", ascending=False).reset_index())

    # Reshaping 1: pivot_table  magnitude class x depth class (event counts)
    tables["pivot_magclass_depthclass"] = pd.pivot_table(
        df, index="magnitude_class", columns="depth_class", values="event_id",
        aggfunc="count", fill_value=0)
    # Reshaping 2: daily event counts per depth class (wide), then melt() back to long form
    daily = pd.pivot_table(df, index="event_date", columns="depth_class",
                           values="magnitude", aggfunc="count", fill_value=0)
    tables["pivot_daily_counts"] = daily
    tables["melt_daily_counts"] = (daily.reset_index()
                                   .melt(id_vars="event_date", var_name="depth_class",
                                         value_name="events"))

    for name, table in tables.items():
        table.to_csv(config.AGG_DIR / f"{name}.csv", index=not isinstance(table.index, pd.RangeIndex))
    return tables


def clean_records(valid_df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    report = {"rows_in": len(valid_df)}
    df = remove_duplicates(valid_df, report)
    df = convert_types(df)
    df = normalize_text(df)
    df = handle_missing(df, report)
    df = add_numpy_features(df)
    df = finalize(df)

    tables = build_aggregations(df)
    report["rows_out"] = len(df)
    report["strong_shallow_events"] = len(tables["filter_strong_shallow"])
    report["regions"] = int(df["region"].nunique())

    df.to_csv(config.CLEANED_PATH, index=False)
    log.info("Cleaning: %d rows in -> %d rows out", report["rows_in"], report["rows_out"])
    return df, report


if __name__ == "__main__":
    config.setup_logging()
    validated = pd.read_csv(config.VALID_PATH)
    cleaned, result = clean_records(validated)
    print(result)
    print(cleaned.dtypes)
    print(cleaned.head())
