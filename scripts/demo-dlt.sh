#!/usr/bin/env bash
# Dead-letter topic demo: an invalid message published to orders.events.v1 ends in orders.events.v1.dlt, with the
# reason in its headers, and the consumer keeps going.
# Usage: make demo-dlt
set -euo pipefail

NOTIFY_URL="${NOTIFY_URL:-http://localhost:8082/services-notify}"
KAFKA_BIN=/opt/kafka/bin
BOOTSTRAP=kafka:19092
marker="demo-dlt-$(date +%s)"

dlt_count() {
  curl -sf "$NOTIFY_URL/actuator/prometheus" | awk '/^notifications_dlt_total\{.*reason="invalid"/ {print $2}'
}

dlt_offsets() {
  docker exec order-kafka "$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic orders.events.v1.dlt
}

echo "== notifications_dlt_total{reason=invalid} antes: $(dlt_count)"
before="$(dlt_offsets)"
echo "== Publicando en orders.events.v1 un mensaje que no es JSON: {$marker"
echo "{$marker" | docker exec -i order-kafka "$KAFKA_BIN/kafka-console-producer.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic orders.events.v1

echo "== Esperando el registro en orders.events.v1.dlt"
for _ in $(seq 1 15); do
  after="$(dlt_offsets)"
  # topic:partition:end-offset lines; the partition whose end offset grew holds the new record at the old end.
  grown="$(join -t: -j1 <(awk -F: '{print $2":"$3}' <<<"$before" | sort) <(awk -F: '{print $2":"$3}' <<<"$after" | sort) \
    | awk -F: '$3 > $2 {print $1":"$2; exit}')"
  if [ -n "$grown" ]; then
    partition="${grown%%:*}"; offset="${grown##*:}"
    record="$(docker exec order-kafka "$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
      --topic orders.events.v1.dlt --partition "$partition" --offset "$offset" --max-messages 1 --timeout-ms 10000 \
      --formatter-property print.headers=true 2>/dev/null)"
    echo "  partición $partition, offset $offset"
    # Headers come first (comma-separated); the stack trace header spans many lines, so only these are shown.
    tr ',' '\n' <<<"$record" | grep -a -E '^kafka_dlt-(original-topic|exception-fqcn|exception-cause-fqcn|exception-message):' \
      | cut -c1-160 | sed 's/^/  /'
    echo "  valor: $(tail -1 <<<"$record" | awk -F'\t' '{print $NF}')"
    grep -aq -- "$marker" <<<"$record" || { echo "El registro nuevo de la DLT no es el publicado" >&2; exit 1; }
    echo "== notifications_dlt_total{reason=invalid} después: $(dlt_count)"
    exit 0
  fi
  sleep 1
done
echo "El mensaje $marker no apareció en orders.events.v1.dlt" >&2
exit 1
