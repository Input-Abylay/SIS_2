#!/usr/bin/env bash
# Collects evidence from the running Kafka broker (topic config, consumer-group offsets,
# one raw message) into results/kafka_evidence.txt.  Used in the report.
# Usage: KAFKA_HOME=~/kafka/kafka_2.13-4.4.0 bash scripts/kafka_evidence.sh
#    or (Docker): KAFKA_DOCKER=sis2-kafka bash scripts/kafka_evidence.sh
set -euo pipefail
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC="${KAFKA_TOPIC:-usgs-earthquakes}"
GROUP="${KAFKA_GROUP_ID:-earthquake-pipeline-consumer}"
OUT="$(dirname "$0")/../results/kafka_evidence.txt"

run() {  # run a Kafka CLI tool either locally or inside the Docker container
  if [[ -n "${KAFKA_DOCKER:-}" ]]; then
    docker exec "$KAFKA_DOCKER" "/opt/kafka/bin/$1" "${@:2}"
  else
    "${KAFKA_HOME:?set KAFKA_HOME or KAFKA_DOCKER}/bin/$1" "${@:2}"
  fi
}

{
  echo "\$ kafka-topics.sh --describe --topic $TOPIC"
  run kafka-topics.sh --describe --topic "$TOPIC" --bootstrap-server "$BOOTSTRAP"
  echo
  echo "\$ kafka-consumer-groups.sh --describe --group $GROUP"
  run kafka-consumer-groups.sh --describe --group "$GROUP" --bootstrap-server "$BOOTSTRAP"
  echo
  echo "\$ kafka-console-consumer.sh --topic $TOPIC --from-beginning --max-messages 1 (key<TAB>value)"
  run kafka-console-consumer.sh --topic "$TOPIC" --from-beginning --max-messages 1 \
      --bootstrap-server "$BOOTSTRAP" --formatter-property print.key=true 2>/dev/null
} > "$OUT" 2>&1
echo "Kafka evidence written to $OUT"
