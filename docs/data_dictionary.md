# Data Dictionary - USGS Earthquake Pipeline

Source: USGS FDSN Event Web Service, `https://earthquake.usgs.gov/fdsnws/event/1/query`
(GeoJSON format). Field definitions follow the official USGS ComCat documentation:
https://earthquake.usgs.gov/data/comcat/data-eventterms.php

## 1. Raw record (`data/raw_data.csv`, Kafka message `payload`)

One row = one seismic event exactly as returned by the API (flattened, not cleaned).

| Field | Type | Kind | Description |
|---|---|---|---|
| `id` | string | identifier | Unique USGS event id, e.g. `us6000u18k` (network code + event code) |
| `mag` | float | numerical | Event magnitude |
| `place` | string | categorical/text | Textual location, e.g. `10 km WSW of Pitaloza Arriba, Panama` |
| `time` | int64 | date/time | Origin time, milliseconds since 1970-01-01 UTC |
| `updated` | int64 | date/time | Last time USGS updated the event, ms since epoch |
| `felt` | float (nullable) | numerical | Number of "Did You Feel It?" reports |
| `cdi` | float (nullable) | numerical | Max community-reported intensity |
| `mmi` | float (nullable) | numerical | Max instrumental intensity (ShakeMap) |
| `alert` | string (nullable) | categorical | PAGER alert: green / yellow / orange / red |
| `status` | string | categorical | `automatic` or `reviewed` (checked by a seismologist) |
| `tsunami` | int (0/1) | categorical | 1 = large oceanic event, tsunami info published |
| `sig` | int | numerical | Significance 0..1000+ (magnitude, felt reports, impact) |
| `net` | string | categorical | Contributing network code (us, ak, pr, nc, ...) |
| `code` | string | identifier | Event code inside the network |
| `nst` | int (nullable) | numerical | Number of seismic stations used |
| `dmin` | float (nullable) | numerical | Distance to nearest station, degrees |
| `rms` | float (nullable) | numerical | Travel-time residual, seconds |
| `gap` | float (nullable) | numerical | Largest azimuthal gap between stations, degrees (0-360) |
| `magType` | string | categorical | Magnitude method (ml, mb, mww, md, mwr, ...) |
| `type` | string | categorical | Event type (earthquake, explosion, landslide, ...) |
| `title` | string | text | Short title, e.g. `M 7.7 - 10 km WSW of ...` |
| `url` | string | text | USGS event page |
| `longitude` | float | numerical | Epicentre longitude, degrees (-180..180) |
| `latitude` | float | numerical | Epicentre latitude, degrees (-90..90) |
| `depth` | float | numerical | Hypocentre depth, km |

Added by the validator only: `is_test_record` (bool) - `True` for the 4 deliberately
invalid test records; `validation_errors` (string) in `data/invalid_records.csv`.

## 2. Processed table `earthquakes` (`data/output.db`, `data/cleaned_data.csv`)

| Column | SQLite type | Origin | Description |
|---|---|---|---|
| `event_id` | TEXT **PRIMARY KEY** | `id` | Unique USGS event id |
| `event_time` | TEXT (ISO-8601 UTC) | `time` | Origin time |
| `updated_time` | TEXT (ISO-8601 UTC) | `updated` | Last update; used by the upsert to keep the newest version |
| `event_date` | TEXT `YYYY-MM-DD` | derived | Date part of `event_time` |
| `event_hour` | INTEGER 0-23 | derived | Hour (UTC) |
| `day_of_week` | TEXT | derived | Monday..Sunday |
| `magnitude` | REAL | `mag` | Magnitude |
| `mag_type` | TEXT | `magType` | Lower-cased magnitude type |
| `magnitude_class` | TEXT | derived (NumPy) | minor <4, light 4-5, moderate 5-6, strong 6-7, major >=7 |
| `depth_km` | REAL | `depth` | Depth, km |
| `depth_class` | TEXT | derived (NumPy) | shallow <70 km, intermediate 70-300 km, deep >=300 km |
| `latitude`, `longitude` | REAL | geometry | Epicentre coordinates |
| `place` | TEXT | `place` | Trimmed location text (`Unknown location` if missing) |
| `region` | TEXT | derived | Country / region extracted from `place`, title-cased, US state codes expanded |
| `event_type` | TEXT | `type` | Lower-cased event type |
| `status` | TEXT | `status` | automatic / reviewed |
| `network` | TEXT | `net` | Lower-cased network code |
| `alert_level` | TEXT | `alert` | PAGER alert, `none` when not issued |
| `tsunami_flag` | INTEGER 0/1 | `tsunami` | Tsunami flag |
| `significance` | INTEGER | `sig` | Significance score |
| `felt_reports` | INTEGER | `felt` | Felt reports (missing -> 0) |
| `station_count` | INTEGER | `nst` | Stations used (missing -> network median) |
| `azimuthal_gap` | REAL | `gap` | Azimuthal gap (missing -> network median) |
| `rms` | REAL | `rms` | RMS residual (missing -> network median) |
| `min_station_distance` | REAL | `dmin` | Nearest station distance (missing -> network median) |
| `energy_joules` | REAL | derived (NumPy) | Radiated energy, `10^(1.5*M + 4.8)` J |
| `log10_energy` | REAL | derived (NumPy) | `1.5*M + 4.8` |
| `distance_to_almaty_km` | REAL | derived (NumPy) | Haversine distance from Almaty (43.24 N, 76.89 E) |
| `url` | TEXT | `url` | USGS event page |

Dropped columns: `cdi`, `mmi` (>88 % missing, only exist for felt events), `title`
(duplicates `mag` + `place`), `code` (part of `event_id`).

## 3. Other outputs

| File | Content |
|---|---|
| `data/raw_data.json` | Untouched API pages (GeoJSON) - original data preserved |
| `data/consumed_data.json` | Records exactly as received by the Kafka consumer |
| `data/validated_data.csv` | Records that passed Pandera |
| `data/invalid_records.csv` | Quarantined invalid records + `validation_errors` |
| `data/aggregations/*.csv` | Filter, GroupBy, pivot_table and melt results |
| `results/validation_failure_cases.csv` | Every individual Pandera failure (record, column, check, value) |
| `results/eda_summary.txt` | EDA statistics |
| `results/pipeline_summary.json` | Record counts of every stage + verification checks |
| `results/pipeline_run_log.txt` | Console output of the last `python main.py` run |
