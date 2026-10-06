# Microservicios con Spring Boot y Kubernetes

El sistema tiene tres servicios reactivos (Spring Boot 4.1, Java 25), cada uno con su propia base de datos, y se comunican por HTTP y por Kafka:

- **[Inventory](inventory-service/README.md)** (`service-inventory`, MySQL 8.4): productos y stock.
- **[Order](order-service/README.md)** (`service-order`, PostgreSQL 17): órdenes. Al confirmarlas descuenta stock en Inventory y publica el resultado en Kafka (`orders.events.v1`) mediante una outbox.
- **[Notification](notify-service/README.md)** (`service-notify`, MongoDB 8.0): consume esos eventos, registra una notificación por evento y la envía por log o por webhook.

## Levantar todo el sistema

Requisitos: Docker con Compose v2 (BuildKit), `make`, `curl` y `jq`. La primera vez, crea `.env` a partir de `.env.example` y rellena las contraseñas: `DB_PASSWORD`, `MYSQL_ROOT_PASSWORD`, `POSTGRES_PASSWORD`, `MONGO_INITDB_ROOT_PASSWORD`, `MONGO_PASSWORD` y `GRAFANA_ADMIN_PASSWORD`. Ningún archivo versionado contiene contraseñas, y Compose se niega a arrancar si falta alguna.

```bash
cp .env.example .env
```

Construye las tres imágenes y levanta las bases de datos, Kafka y los tres servicios. Espera a que todos estén `healthy`:

```bash
make up
```

Lo mismo, más Grafana, Loki, Tempo, Prometheus y `kafka-exporter`:

```bash
make up-observability
```

Las herramientas del perfil `tools` (Kafka UI, el receptor de webhooks y el CLI de Liquibase) se levantan aparte:

```bash
docker compose --profile tools up -d kafka-ui webhook-receiver
```

Detén y elimina los contenedores de todos los perfiles; los volúmenes se conservan:

```bash
make down
```

`make up` equivale a `docker compose up -d --build --wait`. El Makefile además quita del entorno las variables `DB_*` que pueda haber exportado `order-service/.env`, que si no sustituirían a las de MySQL.

### Puertos

Todos se publican solo en `127.0.0.1`.

| Puerto | Servicio | Perfil | URL o uso |
|---|---|---|---|
| 8080 | `service-inventory` | por defecto | <http://localhost:8080/services-inventory> |
| 8081 | `service-order` | por defecto | <http://localhost:8081/services-order> |
| 8082 | `service-notify` | por defecto | <http://localhost:8082/services-notify> |
| 3306 | MySQL (Inventory) | por defecto | `inventory-mysql` |
| 5432 | PostgreSQL (Order) | por defecto | `order-postgres` |
| 27017 | MongoDB (Notification) | por defecto | `notify-mongo` |
| 9092 | Kafka, listener externo | por defecto | `localhost:9092` desde la máquina; los contenedores usan `kafka:19092` |
| 8085 | Kafka UI | `tools` | <http://localhost:8085> |
| 8086 | Receptor de webhooks | `tools` | `http://webhook-receiver:8080/...` desde la red de Compose |
| 3000 | Grafana | `observability` | <http://localhost:3000>, con `GRAFANA_ADMIN_USER` y `GRAFANA_ADMIN_PASSWORD` de `.env` |
| 4317 / 4318 | OTLP gRPC / HTTP | `observability` | colector de trazas y logs |

Cada puerto del host se puede cambiar con su variable en `.env`: `INVENTORY_PORT`, `ORDER_PORT`, `NOTIFY_PORT`, `KAFKA_PORT`, etc.

### Demos

Ejecuta las demos con el sistema arriba (`make up`):

| Comando | Qué hace |
|---|---|
| `make demo-flow` | Crea un producto en Inventory, crea y confirma una orden en Order, y muestra la notificación `sent` que genera Notification |
| `make demo-dlt` | Publica un mensaje que no es JSON en `orders.events.v1` y lo muestra en `orders.events.v1.dlt` con los headers del error. Muestra también `notifications_dlt_total{reason="invalid"}` antes y después |
| `make demo-lag` | Detiene `service-notify`, confirma 10 órdenes (`DEMO_ORDERS`), muestra el lag del grupo `service-notify` por partición y vuelve a arrancar el servicio. Al final el lag queda en 0 y hay una notificación `sent` por orden |
| `make demo-circuit-breaker` | Para Inventory y muestra el circuito de Order pasar a `OPEN`, después a `HALF_OPEN` y vuelta a `CLOSED` |

### Qué levanta Compose

- **Una sola red** (`backend`): los contenedores se llaman entre sí por nombre de servicio (`kafka`, `mongodb`, `service-order`...). No se usa `network_mode` ni `host.docker.internal`.
- **Kafka 4.3 en modo KRaft, un solo nodo**, con dos listeners: `kafka:19092` para los contenedores y `localhost:9092` para la máquina.
- **Imágenes:** todas las imágenes externas están fijadas por versión y digest. Las de los servicios se construyen con la misma estrategia ([ADR 0002](docs/adr/0002-container-image.md)):
  - JRE mínima con `jlink`;
  - base `distroless`;
  - usuario `10001`;
  - caché AOT de Java 25;
  - el contrato de `contracts/` como contexto de build con nombre, sin copiarlo en cada módulo.
- **Perfiles:**
  - por defecto: las bases de datos, Kafka y los tres servicios;
  - `tools`: Kafka UI, Liquibase y el receptor de webhooks;
  - `observability`: Grafana, Loki, Tempo, Prometheus y `kafka-exporter`.

### Observabilidad

`make up-observability` añade dos contenedores:

- **`grafana/otel-lgtm`:** el colector OpenTelemetry, Prometheus, Tempo, Loki y Grafana. Recibe por OTLP las trazas y los logs de los tres servicios y hace scrape de sus métricas por nombre de servicio.
- **`kafka-exporter`:** publica el lag del grupo de consumidores.

Grafana provisiona en la carpeta geovannycode los dashboards de Order (resiliencia) y de Notification (consumidor). Una sola traza recorre `PUT /orders/{id}` → Inventory → outbox → Kafka → Notification → MongoDB (→ webhook).

Para configurar cada servicio y verificarlo, consulta [Inventory Service](inventory-service/README.md), [Order Service](order-service/README.md) y [Notification Service](notify-service/README.md).

Notification también se despliega con `make k8s-up` (MongoDB + Kafka + los tres servicios).
La [guía Kubernetes de Notification](notify-service/README.md#kubernetes-local--fase-10) incluye
las demos de flujo, caída del consumidor, rebalanceo y caída de MongoDB; `make k8s-url` muestra los tres accesos.
