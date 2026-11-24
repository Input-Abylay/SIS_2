# SIS2 - Earthquake Data Streaming & Validation Pipeline

**Course:** Data Collection & Preparation
**Team:** Abylay Almaskhanov, Alibiy Armiyanov
**Data source:** [USGS Earthquake Catalog API](https://earthquake.usgs.gov/fdsnws/event/1/)

An end-to-end pipeline that collects every M2.5+ earthquake of October 2026 from the
USGS API, streams the records through a real **Apache Kafka** broker, validates them with
**Pandera**, cleans and transforms them with **pandas / NumPy**, stores them idempotently in
**SQLite3**, and performs exploratory data analysis with **Matplotlib / Seaborn**.

```
USGS API ──> api_client.py ──> producer.py ──> [Kafka topic: usgs-earthquakes, 3 partitions]
                 │ raw_data.json/csv                         │
                 ▼                                           ▼
                                                       consumer.py ──> validator.py (Pandera)
                                                                          │ valid      │ invalid -> data/invalid_records.csv
                                                                          ▼
                                analysis.py <── loader.py (SQLite3) <── cleaner.py (pandas + NumPy)
                                figures/*.png     data/output.db
```

## 1. Project structure

```
├── main.py                     # runs the whole pipeline in order
├── requirements.txt
├── docker-compose.yml          # Kafka broker (option A)
├── .env.example                # optional configuration (no secrets needed)
├── src/
│   ├── config.py               # paths, API params, Kafka settings
│   ├── api_client.py           # stage 1  USGS API collection
│   ├── producer.py             # stage 2a Kafka producer (separate script)
│   ├── consumer.py             # stage 2b Kafka consumer (separate script)
│   ├── validator.py            # stage 3  Pandera schema + invalid test records
│   ├── cleaner.py              # stage 4  cleaning, NumPy, filter, groupby, pivot/melt
│   ├── loader.py               # stage 5  SQLite3 upsert + idempotency test + SQL checks
│   └── analysis.py             # stage 6  EDA + figures (reads from SQLite)
├── sql/
│   ├── schema.sql              # table definition (PRIMARY KEY, CHECK constraints)
│   └── verification_queries.sql
├── scripts/kafka_evidence.sh   # dumps topic / consumer-group info from the broker
├── data/                       # raw_data.csv, raw_data.json, output.db, ... (generated)
├── figures/                    # generated charts
├── results/                    # pipeline_summary.json, eda_summary.txt, run log
├── docs/data_dictionary.md
└── report/
    ├── project_report.pdf      # final report
    └── project_report.docx     # editable Word version of the report
```

## 2. Requirements

* Python **3.10+** (developed with 3.11)
* Java **17+** (only for option B - local Kafka) or Docker (option A)
* Internet access for the USGS API (no API key needed)

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Kafka broker setup

Kafka 4.x runs in **KRaft mode** (no ZooKeeper). Use **one** of the two options.

### Option A - Docker

```bash
docker compose up -d               # starts broker "sis2-kafka" on localhost:9092
docker compose ps                  # STATUS should be "running"
```

### Option B - local binary (what we used on macOS)

```bash
# download & unpack once
curl -O https://dlcdn.apache.org/kafka/4.4.0/kafka_2.13-4.4.0.tgz
tar xzf kafka_2.13-4.4.0.tgz && cd kafka_2.13-4.4.0

# format storage once
KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties

# start the broker (keep this terminal open)
bin/kafka-server-start.sh config/server.properties
```

### Topic configuration

The producer creates the topic automatically (`usgs-earthquakes`, **3 partitions**,
replication factor 1) through `KafkaAdminClient`. To create / inspect it manually:

```bash
bin/kafka-topics.sh --create --topic usgs-earthquakes --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic usgs-earthquakes --bootstrap-server localhost:9092
# Docker: docker exec sis2-kafka /opt/kafka/bin/kafka-topics.sh --describe --topic usgs-earthquakes --bootstrap-server localhost:9092
```

## 4. Configuration

All settings have working defaults in `src/config.py`. To change them, copy
`.env.example` to `.env` and edit:

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | broker address |
| `KAFKA_TOPIC` | `usgs-earthquakes` | topic name |
| `KAFKA_GROUP_ID` | `earthquake-pipeline-consumer` | consumer group |
| `USGS_START_TIME` / `USGS_END_TIME` | `2026-10-01` / `2026-11-01` | requested period |
| `USGS_MIN_MAGNITUDE` | `2.5` | minimum magnitude |

API request used: `GET https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2026-10-01&endtime=2026-11-01&minmagnitude=2.5&orderby=time-asc&limit=1000&offset=N`
(pages of 1000, total taken from the `/count` endpoint first).

## 5. Running the pipeline

### Everything at once

```bash
python main.py                 # API -> Kafka -> Pandera -> cleaning -> SQLite (x2) -> EDA
python main.py --skip-collect  # same, but re-uses data/raw_data.json (no internet)
```

### Stage by stage (execution order)

```bash
python -m src.api_client       # 1. collect   -> data/raw_data.json, data/raw_data.csv
python -m src.producer         # 2. publish   -> Kafka topic (prints published count)
python -m src.consumer         # 3. consume   -> data/consumed_data.json (prints consumed count)
python -m src.validator        # 4. validate  -> data/validated_data.csv, data/invalid_records.csv
python -m src.cleaner          # 5. clean     -> data/cleaned_data.csv, data/aggregations/
python -m src.loader           # 6. load x2 + SQL checks -> data/output.db
python -m src.analysis         # 7. EDA       -> figures/*.png, results/eda_summary.txt
```

Producer and consumer can also run in two terminals at the same time: start
`python -m src.consumer` first, then `python -m src.producer` in a second terminal - the
consumer prints its count and stops after the END_OF_STREAM markers arrive.

**Consumer termination:** the producer sends one `END_OF_STREAM` marker to every partition;
the consumer stops when it has seen all of them (backup: 15 s without messages).

### Kafka evidence for the report

```bash
KAFKA_HOME=/path/to/kafka_2.13-4.4.0 bash scripts/kafka_evidence.sh   # or KAFKA_DOCKER=sis2-kafka
# -> results/kafka_evidence.txt (topic description, consumer-group offsets, one raw message)
```

## 6. Database verification

```bash
sqlite3 data/output.db < sql/verification_queries.sql     # if the sqlite3 CLI is installed
python -m src.loader                                      # loads twice + runs the same queries
```

Main table: `earthquakes` with `event_id TEXT PRIMARY KEY`. Rows are written with
`INSERT ... ON CONFLICT(event_id) DO UPDATE ... WHERE excluded.updated_time > earthquakes.updated_time`,
so re-running the loader never creates duplicates and only overwrites a row when USGS
published a newer version of that event.

## 7. Expected output

The end of `python main.py` (full log in `results/pipeline_run_log.txt`):

```
                 stage  records
 collected_api_records     2604
    published_to_kafka     2604
   consumed_from_kafka     2604
    test_records_added        4
       validated_total     2608
         valid_records     2604
       invalid_records        4
records_after_cleaning     2604
      stored_in_sqlite     2604

Checks:
  [PASS] published_equals_collected
  [PASS] consumed_equals_published
  [PASS] stored_equals_cleaned
  [PASS] no_test_records_in_db
  [PASS] no_duplicate_ids_in_db
  [PASS] idempotent_reload
```

Exact numbers can change slightly if you call the API again later, because USGS keeps
reviewing events (`--skip-collect` reproduces the committed raw data exactly).

## 8. How to verify a successful run

1. `main.py` exits with code 0 and every check above shows `PASS`.
2. `results/pipeline_summary.json` contains the counts of every stage.
3. `data/invalid_records.csv` lists the 4 `TEST_` records with their errors.
4. `Load #2` prints `0 written` and identical table fingerprints (idempotency).
5. `figures/` contains 6 PNG charts; `results/eda_summary.txt` contains the statistics.
6. `kafka-consumer-groups.sh --describe --group earthquake-pipeline-consumer` shows `LAG 0`.

## 9. Troubleshooting

| Problem | Fix |
|---|---|
| `Kafka broker not reachable at localhost:9092` | start the broker (section 3) |
| `USGS API error 400` | check dates in `.env` (format `YYYY-MM-DD`) |
| consumer waits 15 s and stops | producer was not run - run `python -m src.producer` |
