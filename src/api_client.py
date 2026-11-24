"""Stage 1 - collect earthquake events from the USGS FDSN Event API.

Endpoint : https://earthquake.usgs.gov/fdsnws/event/1/query
Params   : format=geojson, starttime, endtime, minmagnitude, orderby=time-asc,
           limit + offset (pagination)

The untouched API pages are saved to data/raw_data.json and a flattened
(but NOT cleaned) copy to data/raw_data.csv.

Run alone:  python -m src.api_client
"""
import json
import logging
import time

import pandas as pd
import requests

from src import config

log = logging.getLogger(__name__)

# Fields we keep from each GeoJSON feature's "properties" object
PROPERTY_FIELDS = [
    "mag", "place", "time", "updated", "felt", "cdi", "mmi", "alert", "status",
    "tsunami", "sig", "net", "code", "nst", "dmin", "rms", "gap", "magType",
    "type", "title", "url",
]


def build_params(offset: int = 1, limit: int = config.USGS_PAGE_SIZE) -> dict:
    return {
        "format": "geojson",
        "starttime": config.USGS_START_TIME,
        "endtime": config.USGS_END_TIME,
        "minmagnitude": config.USGS_MIN_MAGNITUDE,
        "orderby": "time-asc",
        "limit": limit,
        "offset": offset,      # USGS offsets are 1-based
    }


def request_with_retry(url: str, params: dict) -> dict:
    """GET a URL and return JSON; retries on network errors and 5xx/429 codes."""
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=config.REQUEST_TIMEOUT)
            if response.status_code == 200:
                return response.json()
            if response.status_code in (429, 500, 502, 503, 504):
                log.warning("HTTP %s (attempt %d/%d), retrying...",
                            response.status_code, attempt, config.MAX_RETRIES)
            else:
                # 400 / 404 etc. - bad parameters, retrying will not help
                raise RuntimeError(f"USGS API error {response.status_code}: {response.text[:200]}")
        except (requests.ConnectionError, requests.Timeout) as error:
            log.warning("Network error (attempt %d/%d): %s", attempt, config.MAX_RETRIES, error)
        except ValueError as error:   # invalid JSON body
            log.warning("Invalid JSON (attempt %d/%d): %s", attempt, config.MAX_RETRIES, error)
        time.sleep(2 ** attempt)      # exponential back-off: 2s, 4s, 8s
    raise RuntimeError(f"USGS API request failed after {config.MAX_RETRIES} attempts")


def get_event_count() -> int:
    """Ask the /count endpoint how many events match before downloading them."""
    count_url = config.USGS_URL.replace("/query", "/count")
    params = build_params()
    for key in ("limit", "offset", "orderby"):
        params.pop(key)
    return int(request_with_retry(count_url, params)["count"])


def feature_to_record(feature: dict) -> dict:
    """Flatten one GeoJSON feature into a flat dict (no cleaning applied)."""
    props = feature.get("properties", {})
    longitude, latitude, depth = (feature.get("geometry", {}).get("coordinates") or [None] * 3)[:3]
    record = {"id": feature.get("id")}
    record.update({field: props.get(field) for field in PROPERTY_FIELDS})
    record.update({"longitude": longitude, "latitude": latitude, "depth": depth})
    return record


def collect_earthquakes() -> list[dict]:
    """Download all pages, save the raw data, return the flat records."""
    total = get_event_count()
    log.info("USGS reports %d events for %s .. %s (M >= %.1f)",
             total, config.USGS_START_TIME, config.USGS_END_TIME, config.USGS_MIN_MAGNITUDE)

    raw_pages, records = [], []
    offset = 1
    while offset <= total:
        page = request_with_retry(config.USGS_URL, build_params(offset=offset))
        features = page.get("features", [])
        if not features:
            break
        raw_pages.append(page)
        records.extend(feature_to_record(f) for f in features)
        log.info("Fetched %d features (offset=%d), total so far %d", len(features), offset, len(records))
        offset += len(features)

    if len(records) < 200:
        raise RuntimeError(f"Only {len(records)} records collected - at least 200 are required")

    # Preserve the original data before any cleaning
    config.RAW_JSON_PATH.write_text(json.dumps(raw_pages))
    pd.DataFrame(records).to_csv(config.RAW_CSV_PATH, index=False)
    log.info("Saved raw data: %s and %s", config.RAW_JSON_PATH.name, config.RAW_CSV_PATH.name)
    return records


def load_raw_records() -> list[dict]:
    """Re-read the preserved raw JSON (used by the producer)."""
    if not config.RAW_JSON_PATH.exists():
        raise FileNotFoundError("data/raw_data.json not found - run `python -m src.api_client` first")
    pages = json.loads(config.RAW_JSON_PATH.read_text())
    return [feature_to_record(f) for page in pages for f in page["features"]]


if __name__ == "__main__":
    config.setup_logging()
    collected = collect_earthquakes()
    print(f"Collected API records: {len(collected)}")
