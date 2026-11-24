"""Stage 2a - Kafka Producer: publishes the collected USGS records to a topic.

* creates the topic (3 partitions) if it does not exist yet
* key   = earthquake id (same event always lands in the same partition)
* value = record serialized as JSON (UTF-8 bytes)
* after the data, one END_OF_STREAM marker is sent to EVERY partition so the
  consumer knows when the finite stream is finished

Run alone:  python -m src.producer
"""
import json
import logging
import sys
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, NoBrokersAvailable, TopicAlreadyExistsError

from src import config
from src.api_client import load_raw_records

log = logging.getLogger(__name__)


def json_serializer(value: dict) -> bytes:
    return json.dumps(value).encode("utf-8")


def ensure_topic() -> None:
    """Create the topic if needed (idempotent)."""
    admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                             client_id="sis2-admin")
    try:
        admin.create_topics([NewTopic(name=config.KAFKA_TOPIC,
                                      num_partitions=config.KAFKA_PARTITIONS,
                                      replication_factor=config.KAFKA_REPLICATION_FACTOR)])
        log.info("Created topic '%s' (%d partitions)", config.KAFKA_TOPIC, config.KAFKA_PARTITIONS)
    except TopicAlreadyExistsError:
        log.info("Topic '%s' already exists", config.KAFKA_TOPIC)
    finally:
        admin.close()


def create_producer() -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=json_serializer,
            acks="all",          # wait until the broker has stored the message
            retries=3,           # automatic resend on transient errors
            linger_ms=10,
        )
    except NoBrokersAvailable:
        log.error("Cannot reach Kafka at %s - is the broker running?", config.KAFKA_BOOTSTRAP_SERVERS)
        raise


def publish_records(records: list[dict]) -> dict:
    """Send every record + END markers. Returns publishing statistics."""
    ensure_topic()
    producer = create_producer()
    run_id = uuid.uuid4().hex[:8]
    published, failed = 0, 0

    futures = []
    for record in records:
        message = {"run_id": run_id,
                   "produced_at": datetime.now(timezone.utc).isoformat(),
                   "payload": record}
        try:
            futures.append(producer.send(config.KAFKA_TOPIC, key=str(record["id"]), value=message))
        except (KafkaError, TypeError, ValueError) as error:   # e.g. non-serializable value
            failed += 1
            log.error("Could not send record %s: %s", record.get("id"), error)

    # Wait for broker acknowledgements so we count only really stored messages
    for future in futures:
        try:
            future.get(timeout=30)
            published += 1
        except KafkaError as error:
            failed += 1
            log.error("Delivery failed: %s", error)

    # One END marker per partition -> consumer stops after seeing all of them
    partitions = sorted(producer.partitions_for(config.KAFKA_TOPIC))
    for partition in partitions:
        end_marker = {"type": config.END_OF_STREAM, "run_id": run_id, "total_published": published}
        producer.send(config.KAFKA_TOPIC, key=config.END_OF_STREAM, value=end_marker,
                      partition=partition).get(timeout=30)
    producer.flush()
    producer.close()

    stats = {"run_id": run_id, "published": published, "failed": failed,
             "partitions": len(partitions)}
    log.info("Producer finished: %s", stats)
    return stats


if __name__ == "__main__":
    config.setup_logging()
    try:
        result = publish_records(load_raw_records())
    except (FileNotFoundError, NoBrokersAvailable, KafkaError) as err:
        print(f"Producer error: {err}")
        sys.exit(1)
    print(f"Successfully published messages: {result['published']} (failed: {result['failed']})")
