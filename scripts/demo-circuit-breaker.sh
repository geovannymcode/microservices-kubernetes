#!/usr/bin/env bash
# Circuit breaker demo against the Compose stack: Inventory down -> OPEN -> HALF_OPEN -> CLOSED.
# Usage: make demo-circuit-breaker   (ORDER_URL overrides the default http://localhost:8081/services-order)
set -euo pipefail
# Same reason as the Makefile: order-service/.env exported in this shell must not override the root .env.
unset DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD

ORDER_URL="${ORDER_URL:-http://localhost:8081/services-order}"
CODE="${DEMO_PRODUCT:-AC-1550}"

circuit_state() {
  curl -s "$ORDER_URL/actuator/circuitbreakers" | grep -o '"state":"[A-Z_]*"' | head -1 | cut -d'"' -f4
}

create_order() {
  curl -s -X POST "$ORDER_URL/orders" -H 'Content-Type: application/json' \
    -d "{\"codeProduct\":\"$CODE\",\"quantity\":1}" | grep -o '"id":[0-9]*' | cut -d: -f2
}

echo "== Circuito antes de empezar: $(circuit_state)"
echo "== Deteniendo service-inventory"
docker compose stop service-inventory >/dev/null

echo "== 10 confirmaciones con Inventory caído"
pending_orders=()
for attempt in $(seq 1 10); do
  order="$(create_order)"
  pending_orders+=("$order")
  printf '  PUT /orders/%-4s -> HTTP %s\n' "$order" \
    "$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ORDER_URL/orders/$order")"
done

echo "== /actuator/circuitbreakers"
curl -s "$ORDER_URL/actuator/circuitbreakers"; echo
echo "== Readiness de Order (debe seguir UP): $(curl -s "$ORDER_URL/actuator/health/readiness")"

echo "== Levantando service-inventory"
docker compose start service-inventory >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' service-inventory)" = "healthy" ]; do sleep 2; done
echo "  service-inventory healthy"

echo "== Esperando a que el circuito salga de OPEN (INVENTORY_CB_WAIT, 30 s por defecto)"
while [ "$(circuit_state)" = "OPEN" ]; do sleep 2; done
echo "  circuito: $(circuit_state)"

# HALF_OPEN lets 3 trial calls through (permitted-number-of-calls-in-half-open-state); if they succeed it closes.
echo "== Reintentando 3 órdenes pendientes (llamadas de prueba de HALF_OPEN)"
for order in "${pending_orders[@]:0:3}"; do
  printf '  PUT /orders/%-4s -> HTTP %s\n' "$order" \
    "$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ORDER_URL/orders/$order")"
done
echo "== Circuito al final: $(circuit_state)"
