"""Central configuration: paths, API parameters and Kafka settings.

Every value can be overridden with an environment variable or a local `.env`
file (see `.env.example`). No secrets are needed - the USGS API is public.
"""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv(path: Path) -> None:
    """Minimal .env reader so we don't need an extra dependency."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_dotenv(PROJECT_ROOT / ".env")

# ---------- Folders ----------
DATA_DIR = PROJECT_ROOT / "data"
FIGURES_DIR = PROJECT_ROOT / "figures"
RESULTS_DIR = PROJECT_ROOT / "results"
SQL_DIR = PROJECT_ROOT / "sql"
for folder in (DATA_DIR, FIGURES_DIR, RESULTS_DIR):
    folder.mkdir(exist_ok=True)

# ---------- Files produced by each stage ----------
RAW_JSON_PATH = DATA_DIR / "raw_data.json"            # untouched API response
RAW_CSV_PATH = DATA_DIR / "raw_data.csv"              # flattened, still uncleaned
CONSUMED_PATH = DATA_DIR / "consumed_data.json"       # what the Kafka consumer received
VALID_PATH = DATA_DIR / "validated_data.csv"          # passed Pandera
INVALID_PATH = DATA_DIR / "invalid_records.csv"       # failed Pandera (+ reasons)
CLEANED_PATH = DATA_DIR / "cleaned_data.csv"          # final processed dataset
AGG_DIR = DATA_DIR / "aggregations"
DB_PATH = DATA_DIR / "output.db"
SUMMARY_PATH = RESULTS_DIR / "pipeline_summary.json"
AGG_DIR.mkdir(exist_ok=True)

# ---------- USGS API ----------
USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USGS_START_TIME = os.getenv("USGS_START_TIME", "2026-10-01")
USGS_END_TIME = os.getenv("USGS_END_TIME", "2026-11-01")
USGS_MIN_MAGNITUDE = float(os.getenv("USGS_MIN_MAGNITUDE", "2.5"))
USGS_PAGE_SIZE = 1000          # records per request (API max is 20000)
REQUEST_TIMEOUT = 30           # seconds
MAX_RETRIES = 3

# ---------- Kafka ----------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "usgs-earthquakes")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "earthquake-pipeline-consumer")
KAFKA_PARTITIONS = 3
KAFKA_REPLICATION_FACTOR = 1
CONSUMER_IDLE_TIMEOUT_MS = 15000   # backup stop condition if END markers never arrive
END_OF_STREAM = "END_OF_STREAM"


def setup_logging() -> None:
    """Readable console logs; silence the very chatty kafka-python internals."""
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
                        datefmt="%H:%M:%S")
    logging.getLogger("kafka").setLevel(logging.WARNING)
