#!/usr/bin/env bash
# End-to-end flow against the Compose stack: product in Inventory -> order created and confirmed in Order ->
# event through Kafka -> notification in Notification.
# Usage: make demo-flow   (INVENTORY_URL, ORDER_URL and NOTIFY_URL override the localhost defaults)
set -euo pipefail

INVENTORY_URL="${INVENTORY_URL:-http://localhost:8080/services-inventory}"
ORDER_URL="${ORDER_URL:-http://localhost:8081/services-order}"
NOTIFY_URL="${NOTIFY_URL:-http://localhost:8082/services-notify}"
CODE="DEMO-$(date +%s)"

echo "== Producto $CODE en Inventory (stock 10)"
curl -sf -X POST "$INVENTORY_URL/inventories" -H 'Content-Type: application/json' \
  -d "{\"idProduct\":\"$CODE\",\"nameProduct\":\"Producto de la demo\",\"price\":10.00,\"stock\":10}" | jq -c .

echo "== Orden de 2 unidades en Order"
order="$(curl -sf -X POST "$ORDER_URL/orders" -H 'Content-Type: application/json' \
  -d "{\"codeProduct\":\"$CODE\",\"quantity\":2}" | jq -r .id)"
echo "  orden $order"

echo "== Confirmación (PUT /orders/$order)"
curl -sf -X PUT "$ORDER_URL/orders/$order" | jq -c '{id, status}'
echo "  stock restante: $(curl -sf "$INVENTORY_URL/inventories/$CODE" | jq .stock)"

echo "== Esperando la notificación (outbox -> Kafka -> Notification)"
for _ in $(seq 1 30); do
  notification="$(curl -sf "$NOTIFY_URL/notify?orderId=$order" | jq -c '.[0] // empty')"
  if [ -n "$notification" ] && [ "$(jq -r .status <<<"$notification")" = "sent" ]; then
    jq . <<<"$notification"
    exit 0
  fi
  sleep 1
done
echo "La notificación de la orden $order no llegó a sent en 30 s" >&2
exit 1
