"""Stage 2b - Kafka Consumer: reads earthquake messages from the topic.

Termination condition (finite stream):
  1. stop when an END_OF_STREAM marker has been received from every partition, or
  2. stop if no message arrives for CONSUMER_IDLE_TIMEOUT_MS (safety net).

Offsets are committed manually after processing, so a restarted consumer in the
same group continues where it stopped instead of re-reading everything.

Run alone:  python -m src.consumer            (consume the next finished stream)
            python -m src.consumer --run-id X (only accept messages of producer run X)
"""
import argparse
import json
import logging
import sys
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

from src import config

log = logging.getLogger(__name__)


def json_deserializer(raw: bytes):
    """Bytes -> dict. Returns None for corrupted messages instead of crashing."""
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def consume_records(run_id: str | None = None) -> tuple[list[dict], dict]:
    try:
        consumer = KafkaConsumer(
            config.KAFKA_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=config.KAFKA_GROUP_ID,
            auto_offset_reset="earliest",     # new group -> start from the beginning
            enable_auto_commit=False,
            value_deserializer=json_deserializer,
            # one fetch can carry up to 1 MB per partition x 3 partitions -> allow bigger frames
            receive_message_max_bytes=10 * 1024 * 1024,
        )
    except NoBrokersAvailable:
        log.error("Cannot reach Kafka at %s - is the broker running?", config.KAFKA_BOOTSTRAP_SERVERS)
        raise

    records, corrupted, skipped_other_runs = [], 0, 0
    finished_partitions: set[int] = set()
    expected_published = None
    total_partitions = None
    last_message_time = time.monotonic()

    try:
        while True:
            batch = consumer.poll(timeout_ms=1000)
            if not batch:
                idle_ms = (time.monotonic() - last_message_time) * 1000
                if idle_ms > config.CONSUMER_IDLE_TIMEOUT_MS:
                    log.warning("No messages for %.0f s - stopping (idle timeout)", idle_ms / 1000)
                    break
                continue
            last_message_time = time.monotonic()

            for topic_partition, messages in batch.items():
                for message in messages:
                    value = message.value
                    if value is None:
                        corrupted += 1
                        continue
                    if run_id and value.get("run_id") != run_id:
                        skipped_other_runs += 1
                        continue
                    if value.get("type") == config.END_OF_STREAM:
                        run_id = run_id or value["run_id"]   # lock onto this stream
                        finished_partitions.add(topic_partition.partition)
                        expected_published = value["total_published"]
                        continue
                    if "payload" not in value:
                        corrupted += 1
                        continue
                    records.append(value["payload"])
            consumer.commit()

            total_partitions = total_partitions or len(consumer.partitions_for_topic(config.KAFKA_TOPIC))
            if len(finished_partitions) == total_partitions:
                log.info("END_OF_STREAM received from all %d partitions", total_partitions)
                break
    except KafkaError as error:
        log.error("Kafka error while consuming: %s", error)
        raise
    finally:
        consumer.close()

    stats = {"run_id": run_id, "consumed": len(records), "expected": expected_published,
             "corrupted": corrupted, "skipped_other_runs": skipped_other_runs}
    config.CONSUMED_PATH.write_text(json.dumps(records))
    log.info("Consumer finished: %s", stats)
    return records, stats


if __name__ == "__main__":
    config.setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", help="only accept messages from this producer run")
    args = parser.parse_args()
    try:
        _, result = consume_records(args.run_id)
    except (NoBrokersAvailable, KafkaError) as err:
        print(f"Consumer error: {err}")
        sys.exit(1)
    print(f"Successfully consumed messages: {result['consumed']} (producer reported {result['expected']})")
