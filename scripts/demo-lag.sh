#!/usr/bin/env bash
# Consumer lag demo: with service-notify stopped, confirmed orders pile up in orders.events.v1 (lag of the
# service-notify group); started again, it consumes them and the lag goes back to 0.
# Usage: make demo-lag   (DEMO_ORDERS sets how many orders, 10 by default)
set -euo pipefail
# Same reason as the Makefile: order-service/.env exported in this shell must not override the root .env.
unset DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD

INVENTORY_URL="${INVENTORY_URL:-http://localhost:8080/services-inventory}"
ORDER_URL="${ORDER_URL:-http://localhost:8081/services-order}"
NOTIFY_URL="${NOTIFY_URL:-http://localhost:8082/services-notify}"
ORDERS="${DEMO_ORDERS:-10}"
CODE="LAG-$(date +%s)"

group_lag() {
  docker exec order-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 \
    --describe --group service-notify 2>/dev/null | awk '$2 == "orders.events.v1" {print "  partición " $3 ": lag " $6; total += $6} END {print "  total: " total}'
}

curl -sf -X POST "$INVENTORY_URL/inventories" -H 'Content-Type: application/json' \
  -d "{\"idProduct\":\"$CODE\",\"nameProduct\":\"Producto de la demo de lag\",\"price\":1.00,\"stock\":$ORDERS}" >/dev/null

echo "== Deteniendo service-notify"
docker compose --progress quiet stop service-notify

echo "== Confirmando $ORDERS órdenes de $CODE"
orders=()
for _ in $(seq 1 "$ORDERS"); do
  order="$(curl -sf -X POST "$ORDER_URL/orders" -H 'Content-Type: application/json' \
    -d "{\"codeProduct\":\"$CODE\",\"quantity\":1}" | jq -r .id)"
  curl -sf -o /dev/null -X PUT "$ORDER_URL/orders/$order"
  orders+=("$order")
done
echo "  órdenes ${orders[*]}"

# The outbox relay publishes every second: give it a moment before reading the group.
sleep 3
echo "== Lag del grupo service-notify con el servicio detenido"
group_lag

echo "== Arrancando service-notify"
docker compose --progress quiet start service-notify
until [ "$(docker inspect -f '{{.State.Health.Status}}' service-notify)" = "healthy" ]; do sleep 2; done
echo "  service-notify healthy"

echo "== Esperando a que el grupo se ponga al día"
for _ in $(seq 1 30); do
  [ "$(group_lag | awk '/total/ {print $2}')" = "0" ] && break
  sleep 1
done
group_lag

sent=0
for order in "${orders[@]}"; do
  sent=$((sent + $(curl -sf "$NOTIFY_URL/notify?orderId=$order&status=sent" | jq length)))
done
echo "== Notificaciones sent de esas $ORDERS órdenes: $sent"
[ "$sent" -eq "$ORDERS" ]
