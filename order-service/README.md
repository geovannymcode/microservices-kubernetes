# Order Service

Microservicio reactivo de órdenes con Java 25, Spring Boot 4.1.1, WebFlux y R2DBC sobre PostgreSQL. Registrará órdenes y descontará stock en Inventory; implementa el contrato completo (v1.1.0): registrar, consultar, listar (JSON, NDJSON y SSE) y confirmar órdenes, con descuento de stock en Inventory protegido con Resilience4j y errores `ProblemDetail`.

## Requisitos

- JDK 25 (`java -version`).
- Maven 3.9.16 mediante el wrapper (`./mvnw`), con verificación de checksum.
- Docker: PostgreSQL 17 con `docker compose` para arrancar la app, y Testcontainers para los tests.

## Arranque local

1. **Variables.** Hay dos `.env`, ambos ignorados por Git:
   - el de la raíz, que usa Compose para crear PostgreSQL (`POSTGRES_*`);
   - el de `order-service`, que usa la app (`DB_*`).

   `POSTGRES_PASSWORD` y `DB_PASSWORD` deben ser iguales. No se reutilizan los `DB_*` de la raíz porque son los de MySQL (Inventory).

   ```sh
   cp .env.example .env                                  # en la raíz: completa POSTGRES_PASSWORD
   cp order-service/.env.example order-service/.env      # completa DB_PASSWORD con el mismo valor
   ```

2. **Base de datos**, desde la raíz:

   ```sh
   docker compose up -d postgresql kafka
   docker compose ps postgresql kafka                    # healthy
   ```

3. **Aplicación**, desde `order-service`:

   ```sh
   set -a; . ./.env; set +a
   ./mvnw spring-boot:run -Dspring-boot.run.profiles=local
   ```

   Al arrancar, Liquibase aplica el changelog `db/changelog/master.yaml` con el contexto `${LIQUIBASE_CONTEXTS:local}`. En `local` crea `order_shop` y tres órdenes `pending` de `AC-1550`. La columna `reference` solo existe con `LIQUIBASE_CONTEXTS=cert` o `prod`, y la entidad no la mapea.

## Configuración

Todas las conexiones se leen de variables de entorno; no hay credenciales en el repositorio:

| Variable | Valor por defecto |
|---|---|
| `DB_HOST` / `DB_PORT` | `localhost` / `5432` |
| `DB_NAME` | `orderdb` |
| `DB_USER` / `DB_PASSWORD` | `order` / (vacío) |
| `LIQUIBASE_CONTEXTS` | `local` |
| `INVENTORY_BASE_URL` | `http://localhost:8080/services-inventory` |
| `INVENTORY_CB_WAIT` | `30s` (tiempo del circuito abierto) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` (desde contenedores de Compose: `kafka:19092`) |
| `OUTBOX_POLL_INTERVAL` / `OUTBOX_BATCH_SIZE` | `1s` / `50` |
| `OUTBOX_RETENTION` | `7d` (antigüedad a partir de la cual se borran los eventos ya publicados) |

## Base de datos y Liquibase

- **Esquema:** `order_shop` con identity, `CHECK (quantity BETWEEN 1 AND 5)`, `CHECK (status_order IN ('pending','completed','canceled'))`, índice por `status_order` y `version` para bloqueo optimista.
- **Por qué hay JDBC:** Liquibase solo funciona sobre JDBC (`spring.liquibase.url`). Usa una conexión bloqueante solo al arrancar, para migrar; el acceso de la app es por R2DBC.
- **CLI:** usa el mismo changelog; los comandos y el orden seguro están en [liquibase-env/README.md](liquibase-env/README.md).

```sh
docker compose run --rm liquibase status    # desde la raíz, tras arrancar la app: "is up to date"
```

## Contrato y API (API-first)

- **Contrato:** `../contracts/services-order.yaml` (OpenAPI 3.0.3, v1.1.0) es la fuente de verdad. No se copia a `src/main/resources`.
- **Generación:** `openapi-generator-maven-plugin` 7.25.0 (ejecución `order-server`) genera con el patrón delegate en `target/generated-sources/openapi`, nunca versionado:
  - `OrdersApi`, `OrdersApiController` y `OrdersApiDelegate`, en `com.geovannycode.order.generated.api`;
  - los DTO, en `com.geovannycode.order.generated.dto`.
- **Delegate propio:** el único código escrito a mano para el API es `order/api/OrdersApiDelegateImpl`. Hasta la fase 7, todas las operaciones responden 501, pero la validación generada ya actúa: `POST /orders` con `{"codeProduct": null}` → 400.

```sh
curl -i http://localhost:8081/services-order/orders                                                         # 501
curl -i -H 'Content-Type: application/json' -d '{"codeProduct": null}' http://localhost:8081/services-order/orders   # 400
```

Postman: carpeta `Order Service / local` con el environment `postman/order-local.postman_environment.json` (`domainOS = localhost:8081`).

## Dominio y persistencia

- **`order/domain`:** `OrderStatus` (`PENDING`, `COMPLETED`, `CANCELED`; con `value()` en minúscula y `fromValue`), `CancelReason`, `OrderNotFoundException` e `IllegalOrderStateException`. Sin dependencias de Spring.
- **`OrderEntity`:** record inmutable sobre `order_shop`.
  - Fechas `Instant` con `@CreatedDate`/`@LastModifiedDate` (`@EnableR2dbcAuditing`) y `@Version` para bloqueo optimista.
  - `complete()` y `cancel(reason)` devuelven una copia; solo una orden `PENDING` puede cambiar de estado.
  - No mapea `reference`, que solo existe en cert/prod.
- **Conversión del estado:** `R2dbcCustomConversions` guarda `OrderStatus` como su `value` en minúscula, el que exige el CHECK de la tabla; Spring Data, por defecto, escribiría `PENDING`.
- **`OrderMapper`** (MapStruct 1.6.3, bean de Spring): `quantity` toma 1 si viene null y las fechas se exponen como `OffsetDateTime` en UTC.

## Casos de uso

`OrderService` es una sola clase y no usa `@Transactional`: nunca se mantiene una transacción abierta mientras se espera a Inventory, y una cancelación debe persistir aunque la respuesta sea un error.

`PUT /orders/{id}` (confirmar):

| Situación | Resultado |
|---|---|
| Orden inexistente | 404 `order-not-found` |
| `completed` | 200 con la orden, **sin** llamar a Inventory |
| `canceled` | 409 `order-canceled`, `reason: ORDER_CANCELED` |
| `pending` + Inventory descuenta | `completed`, 200 |
| `pending` + rechazo de Inventory | `canceled` con `cancelReason` (persistido), 409 `order-rejected`, `reason: PRODUCT_NOT_FOUND` o `INSUFFICIENT_STOCK` |
| `pending` + Inventory no disponible | sigue `pending`, 503 `inventory-unavailable` con `Retry-After` = `INVENTORY_CB_WAIT` (30 s) |

- **Confirmaciones simultáneas:** si dos confirman a la vez, la que pierde la carrera de `@Version` relee y vuelve a aplicar la tabla (máximo 2 relecturas). Como la `Idempotency-Key` es `order-<id>`, Inventory descuenta una sola vez y ambas responden 200.
- **`POST /orders`:** 201 con `Location`, siempre en `pending`, sin llamar a Inventory. El código de producto se guarda en mayúsculas (`ac-1550` → `AC-1550`), que es lo que acepta Inventory.
- **`GET /orders`:** acepta `?status=` y responde en JSON, NDJSON o SSE (`event: order`, `id` = id de la orden), según el `Accept`.
- **Errores:** `ProblemDetail` con `type` `https://geovannycode.com/problems/<slug>`, `title`, `status`, `detail`, `instance`, `timestamp` y `errors[]` en validación; el mismo formato que Inventory. El JSON también es estricto: `quantity: 1.9` o `"2"` → 400.
- **Swagger UI:** `/services-order/swagger-ui.html` muestra el contrato estático (`/services-order/openapi/services-order.yaml`).
- **CORS:** solo en el perfil `local`, para los viewers HTML, igual que en Inventory.

## Cliente de Inventory y resiliencia

- **Cliente:** se genera desde `../contracts/services-inventory.yaml` (`spring-http-interface`, reactivo) y se registra con `@ImportHttpServices`. URL y timeouts salen de `inventory.client`: `base-url` es `${INVENTORY_BASE_URL}`, la conexión 1 s y la respuesta 2 s.
- **`InventoryGateway.reserveStock(orderId, codeProduct, quantity)`:** es la única operación. Envía `Idempotency-Key: order-<id>`, así que un reintento nunca descuenta dos veces.
- **Traducción de respuestas de Inventory**, antes de la resiliencia:

  | Respuesta | Resultado | ¿Se reintenta? |
  |---|---|---|
  | 404 | `InventoryRejectedException(PRODUCT_NOT_FOUND)` | No |
  | 409 | `InventoryRejectedException(INSUFFICIENT_STOCK)` | No |
  | Otro 4xx | `IllegalStateException` (error de integración) | No |
  | 5xx, timeout, conexión | Fallo técnico | Sí |

  Al final, todo fallo técnico (incluidos circuito abierto y rate limit) llega como `InventoryUnavailableException`.

- **Resilience4j 2.4.0, instancia `inventory`.** Los operadores se aplican de dentro hacia fuera:

  | Operador | Configuración |
  |---|---|
  | TimeLimiter | 2 s por intento |
  | RateLimiter | 50/s |
  | CircuitBreaker | Ventana de 10, mínimo 5 llamadas, 50 % de fallos o lentas, `INVENTORY_CB_WAIT` (30 s) abierto |
  | Retry | 3 intentos, backoff exponencial de 500 ms ×2 con jitter, solo fallos técnicos |

  Los rechazos de negocio no cuentan como fallos del circuito. El perfil `local` reduce la ventana a 5 y el rate limiter a 10 por minuto para la demo. No se usa Spring Cloud.
- **Actuator:** `/services-order/actuator/circuitbreakers` y `/circuitbreakerevents`.

## Eventos en Kafka (outbox)

Order publica `OrderCompleted` y `OrderCanceled` en el topic `orders.events.v1` para Notification. El contrato está en [`contracts/events/order-events.yaml`](../contracts/events/order-events.yaml) (AsyncAPI 3.1).

**Por qué outbox.** Guardar la orden y luego publicar son dos sistemas distintos; un fallo entre ambos pasos deja un evento perdido o un evento de un cambio que no se guardó. Por eso `OrderService` no publica en Kafka:

1. Inventory responde (fuera de cualquier transacción).
2. En **una** transacción reactiva (`TransactionalOperator`): `save` de la orden (`completed` o `canceled`) e `INSERT` en `outbox_event`. Si cualquiera falla, se deshacen ambos (un conflicto de versión, por ejemplo, no deja evento).
3. `OutboxRelay` publica después lo que esté pendiente.

**Relay.** Cada `OUTBOX_POLL_INTERVAL`:

- Bloquea un lote con `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 50`: con varias réplicas, cada una toma filas distintas sin esperar a las otras.
- Envía los eventos en orden con `KafkaTemplate` (`Mono.fromFuture`, en `boundedElastic`, porque `send` puede bloquear mientras busca metadatos).
- Marca `published_at` solo en los que Kafka confirmó y hace commit. Si un envío falla, el lote se corta ahí y el resto se reintenta en el siguiente ciclo. Si el lote salió completo, encadena otro de inmediato.

Una vez por hora borra los eventos publicados con más de `OUTBOX_RETENTION`.

**Entrega al menos una vez.** Si el relay cae después del ack de Kafka y antes del commit, el evento se reenvía. El consumidor deduplica por `eventId`, como indica el AsyncAPI.

**Mensaje.**

- Key: id de la orden, lo que garantiza el orden por orden.
- Headers: `eventType` y `eventId`.
- Valor: la envoltura JSON (`eventId`, `eventType`, `occurredAt`, `version: 1` y `data`), serializada con Jackson 3 al escribirla en la outbox. Se guarda en `payload` (JSONB) y el relay publica ese contenido sin volver a serializar. JSONB normaliza el orden de las claves y los espacios, así que el consumidor no debe depender del orden de los campos.

**Productor.** `acks=all`, `enable.idempotence=true` y `compression.type=zstd`. Se usa spring-kafka puro: Reactor Kafka está descontinuado.

**Topic.** Lo crea un `NewTopic` (3 particiones, réplica 1) solo en los perfiles `local`, `docker` y `test`. En Kubernetes el topic es un recurso de la plataforma, y el broker de Compose tiene la autocreación desactivada.

**Kafka caído.** Order sigue registrando y confirmando órdenes, y los eventos esperan en la outbox. Readiness incluye solo `readinessState`; Kafka no tiene indicador de salud en Boot y queda fuera a propósito.

**Probarlo a mano**, desde la raíz:

```sh
docker compose up -d kafka
docker compose --profile tools up -d kafka-ui          # http://localhost:8085 -> Topics -> orders.events.v1
docker exec order-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
  --topic orders.events.v1 --from-beginning --property print.key=true --property print.headers=true
```

Confirma una orden (`PUT /services-order/orders/{id}`) y aparece un `OrderCompleted`. Con un producto inexistente aparece un `OrderCanceled` con `cancelReason: PRODUCT_NOT_FOUND`. Para ver los eventos pendientes:

```sh
docker exec order-postgres psql -U order -d orderdb -c \
  "SELECT event_type, aggregate_id, created_at, published_at FROM outbox_event ORDER BY created_at DESC LIMIT 10"
```

**Mejoras opcionales (no incluidas):** Schema Registry con Avro o JSON Schema para validar el contrato en el broker, y CDC con Debezium en lugar de polling.

## Observabilidad

Mismas decisiones que Inventory (fase 7). Aquí se documenta lo propio de Order.

### Actuator y salud

Actuator va en el mismo puerto (8081), bajo `/services-order/actuator`, y expone solo `health`, `info`, `prometheus`, `metrics`, `circuitbreakers`, `circuitbreakerevents`, `retries` y `ratelimiters`. `env`, `beans` y `configprops` responden 404.

| Grupo | Incluye | Por qué |
|---|---|---|
| `liveness` | `livenessState` | Reiniciar no arregla ni la BD ni Inventory. |
| `readiness` | `readinessState`, `r2dbc` | Sin BD, Order no puede atender. **No** incluye el circuit breaker ni Kafka: con el circuito abierto, Order responde 503 rápido y sigue registrando órdenes; con Kafka caído, los eventos esperan en la outbox. |
| `resilience` | `circuitBreakers`, con detalles siempre | Muestra, solo como información, el estado del circuito (`/actuator/health/resilience` → `details.inventory.details.state`) sin hacer públicos los detalles del resto de componentes. |

En `/actuator/health` el componente `circuitBreakers` aparece como `UP` con el circuito cerrado y como `UNKNOWN` con el circuito abierto o semiabierto (`allow-health-indicator-to-fail: false`). Nunca aparece como `DOWN`.

`/actuator/info` incluye `build` (artefacto, versión, fecha) y `git` (rama y commit).

### Métricas (`/actuator/prometheus`)

- **HTTP:** `http_server_requests_seconds` con histograma y buckets SLO (100 ms, 300 ms, 1 s), y `http_client_requests_seconds` con histograma para las llamadas a Inventory. Todas las series llevan `application="service-order"`.
- **Resilience4j** (instancia `inventory`, vía `resilience4j-micrometer`):
  - `resilience4j_circuitbreaker_state`, `..._calls_seconds`, `..._failure_rate` y `..._not_permitted_calls_total`;
  - `resilience4j_retry_calls_total`;
  - `resilience4j_ratelimiter_available_permissions`;
  - `resilience4j_timelimiter_calls_total`.
- **Negocio:**
  - `orders_registered_total`: órdenes registradas. Se llama `orders.registered` y no `orders.created`, porque el cliente de Prometheus elimina el sufijo `_created`, reservado en OpenMetrics, y la serie saldría como `orders_total`.
  - `orders_confirmed_total{result="completed|canceled|unavailable"}`: solo cuenta cambios de estado y respuestas 503. Reconfirmar una orden completada no suma. Las tres series existen desde el arranque.
  - `outbox_pending`: eventos sin publicar. Lo actualiza el relay en cada ciclo; si crece, Kafka está caído o el relay no da abasto. Se registra en `OutboxRelay` y no en `OrderService`, porque es quien conoce la tabla.

Cada transición del circuito se registra en INFO, por ejemplo `Circuito hacia Inventory: CLOSED -> OPEN`.

### Trazas

`spring-boot-starter-opentelemetry` exporta por OTLP a `${OTEL_EXPORTER_OTLP_ENDPOINT:http://localhost:4318}/v1/traces`, con un muestreo de `${TRACING_SAMPLING:1.0}`. `spring.reactor.context-propagation=auto` mantiene `traceId` y `spanId` en el MDC entre saltos de hilo.

Una confirmación produce **una sola traza**:

```
http put /orders/{orderId}                         service-order     SERVER
├─ query  SELECT order_shop                        service-order     (r2dbc-proxy)
├─ http put                                        service-order     CLIENT  (uno por intento)
│  └─ http put /inventories/{productId}            service-inventory SERVER
│     ├─ query  INSERT stock_movements             service-inventory
│     ├─ query  UPDATE products                    service-inventory
│     └─ query  SELECT products                    service-inventory
├─ query  UPDATE order_shop                        service-order
├─ query  INSERT outbox_event                      service-order
└─ outbox relay orders.events.v1                   service-order     INTERNAL
   └─ orders.events.v1 send                        service-order     PRODUCER
```

- **Hacia Inventory:** el cliente HTTP declarativo se construye sobre el `WebClient.Builder` de Boot, así que lleva la observación y envía `traceparent`. Cada reintento es un span `CLIENT` distinto dentro de la misma traza, porque `Retry` vuelve a suscribir la llamada.
- **Hacia Kafka:** `spring.kafka.template.observation-enabled=true` crea el span `PRODUCER` e inyecta `traceparent` en los headers del mensaje. Como el relay publica en otro momento y en otro hilo, la outbox guarda el `traceparent` de la petición (columna `trace_parent`, changeSet 004). El relay publica dentro de un span hijo de esa traza, así que el evento de Kafka continúa la traza de la confirmación, y Notification podrá continuarla también.

### Logs

Los perfiles `docker` y `k8s` escriben JSON ECS con `trace.id` y `span.id`, igual que Inventory. El mismo `trace.id` aparece en los logs de los dos servicios para una petición.

Además, `OpenTelemetryLogsConfiguration` conecta Logback al SDK de OpenTelemetry (`opentelemetry-logback-appender-1.0`, sin `logback-spring.xml`), y con `OTEL_LOGS_EXPORTER=otlp` los logs salen por OTLP hacia Loki con su traza. Compose y Kubernetes lo activan; en local queda apagado (`management.logging.export.otlp.enabled=false`), porque no hay colector.

### Stack local y dashboard

El profile `observability` de Compose levanta `grafana/otel-lgtm` (con credenciales de `.env`) y `kafka-exporter`. El `prometheus.yaml` que se monta (`observability/prometheus/prometheus.yaml`) es el de la imagen más un scrape cada 5 s de cada servicio por su nombre en Compose (`service-inventory:8080`, `service-order:8081`, `service-notify:8082`) y del exportador. Ya no hay target `host.docker.internal`: para ver Order en Grafana tiene que correr en Compose.

Grafana provisiona [`observability/dashboards/order-resilience.json`](../observability/dashboards/order-resilience.json), en la carpeta geovannycode, con estos paneles:

- estado del circuito como línea de tiempo;
- tasa de fallos con el umbral del 50 %;
- llamadas por resultado, incluidas las rechazadas;
- reintentos;
- latencia p95 hacia Inventory;
- órdenes por resultado y totales;
- eventos pendientes en la outbox.

```sh
INVENTORY_CB_WAIT=15s make up-observability      # desde la raíz: los tres servicios y el stack
```

**Demo del circuito**, con el dashboard abierto en <http://localhost:3000> (Dashboards → geovannycode → *Order — Resiliencia hacia Inventory*):

1. `docker stop service-inventory` y confirma dos o tres órdenes: responden 503 con `Retry-After` y el circuito pasa a **OPEN**. `/actuator/health/readiness` sigue `UP`.
2. Pasados 15 s (`INVENTORY_CB_WAIT`) el circuito pasa solo a **HALF_OPEN**.
3. `docker start service-inventory` y, cuando esté healthy, confirma una orden pendiente: 200 y el circuito vuelve a **CLOSED**.

Para ver la traza, busca en Explore → Tempo el trace ID que envíes en `traceparent`:

```sh
curl -X PUT -H 'traceparent: 00-4bf92f3577b34da6a3ce929d0e0e47a1-00f067aa0ba902b7-01' \
  http://localhost:8081/services-order/orders/<id>
```

## Contenedor

La imagen sigue la misma estrategia que service-inventory ([ADR 0002](../docs/adr/0002-container-image.md)):

- **Etapas:** Dockerfile multi-stage, `jlink` con una JRE mínima, base `gcr.io/distroless/cc-debian13`, capas de Spring Boot sin `--launcher` y AOT cache de Java 25.
- **Ejecución:** usuario `10001:10001`, `JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"` y `EXPOSE 8081`.
- **Lo que no lleva:** ni JDK, ni código fuente, ni `.env`, ni `liquibase-env/` (ver `.dockerignore`).

**Contratos.** El build necesita `contracts/services-order.yaml` (servidor) y `contracts/services-inventory.yaml` (cliente), que están fuera de `order-service/`. Igual que en Inventory, llegan como **contexto de build con nombre** y no se copian dentro del módulo:

```sh
docker build --build-context contracts=contracts -t geovannycode/service-order:0.0.1-SNAPSHOT order-service   # desde la raíz
```

No se usa la raíz del repo como contexto: enviaría al daemon el repo entero (incluidos los `target/` de ambos servicios) y la imagen de Order se construiría distinto que la de Inventory. Compose (`additional_contexts`) y CI (`build-contexts`) pasan el mismo contexto.

**Training run del AOT cache.** No necesita PostgreSQL, Kafka ni Inventory:

```
java -XX:AOTCacheOutput=/app/app.aot -Dspring.context.exit=onRefresh -Dspring.liquibase.enabled=false -jar /app/app.jar
```

- **`spring.context.exit=onRefresh`:** crea los beans y termina antes de arrancar los beans de ciclo de vida. Así no se levanta Netty, ni el relay de la outbox, ni el productor de Kafka.
- **`spring.liquibase.enabled=false`:** evita la migración, la única conexión que se abriría durante el refresh.
- **Pool de R2DBC:** no conecta hasta el primer uso.
- **`KafkaAdmin`:** sin perfil activo no hay `NewTopic`, así que no intenta crear el topic.

**JRE.** Lleva los mismos módulos que la de Inventory más `java.security.sasl`, que el cliente de Kafka enlaza (calculado con `jdeps`).

**Peso.** Se excluyen las dependencias que no se usan:

- los binarios nativos de QUIC/HTTP/3 (~12 MB, que llegan por WebFlux y por el WebClient);
- los códecs snappy y lz4 de Kafka (el productor solo comprime con zstd).

| Medición (Apple Silicon, Docker Desktop) | Valor |
|---|---|
| Tamaño (`docker image ls`) | 280 MB: AOT cache 100 MB, dependencias 76 MB, JRE 65 MB y base distroless ~37 MB. Inventory pesa 247 MB; la diferencia son Kafka (+zstd), Liquibase y el driver JDBC. |
| Arranque con AOT cache (mediana de 3, contra Postgres y Kafka de Compose) | 2,08 s |
| Arranque sin AOT cache (`-XX:AOTMode=off`) | 3,54 s (−41 % con cache) |
| Trivy 0.75.0, HIGH/CRITICAL corregibles | 0, tras subir SCRAM (transitivo de `r2dbc-postgresql`) de 3.2 a 3.4 por CVE-2026-53712 |

**Usuario.** `docker run --rm <img> id` no funciona: la imagen distroless no tiene `id` ni shell, y el `ENTRYPOINT` es `java`. Se comprueba así:

```sh
docker image inspect geovannycode/service-order:0.0.1-SNAPSHOT --format '{{.Config.User}}'   # 10001:10001
docker top service-order -eo uid,pid,args                                                    # UID 10001
```

**Build en CI (BuildKit con driver `docker-container`).** El Dockerfile resuelve dos problemas que no aparecían en las builds locales:

- **`unzip` en la etapa de build.** La imagen `eclipse-temurin:25-jdk` no lo trae, y sin él `mvnw` descarga Maven en `.tar.gz` en lugar del `.zip`. El hash fijado en `maven-wrapper.properties` es el del `.zip` (el que exige `mvnw.cmd` en Windows), así que la validación fallaba. En local no pasaba porque Maven ya estaba en la caché `/root/.m2`.
- **`OTEL_TRACES_EXPORTER=none` en la etapa `aot`.** El builder de `setup-buildx-action` inyecta en cada `RUN` variables `OTEL_*` con un endpoint `unix://` para su propio tracing. Boot les da prioridad sobre cualquier propiedad y el training run fallaba al crear el exportador OTLP. Un `ENV` del Dockerfile prevalece sobre lo inyectado, y esa etapa no llega a la imagen final.

Para reproducir la build de CI en local, con una caché vacía:

```sh
docker buildx create --name ci --driver docker-container
docker buildx build --builder ci --platform linux/amd64 --build-context contracts=contracts --load order-service
```

### Sistema completo con Compose

Sin profile, `docker compose up` levanta MySQL, PostgreSQL, Kafka, service-inventory y service-order. Los profiles `observability` (Grafana/Tempo/Prometheus) y `tools` (kafka-ui, Liquibase CLI) se levantan aparte.

- **Arranque:** `service-order` espera a que PostgreSQL, Kafka y service-inventory estén healthy.
- **Healthcheck:** consulta `/services-order/actuator/health/readiness` con la misma clase `HealthCheck` de 1,5 KB que Inventory.
- **Variables:** `DB_PASSWORD` sale de `POSTGRES_PASSWORD` del `.env` raíz. `DB_HOST` no se pasa: el perfil `docker` usa por defecto `postgresql`, `kafka:19092` y `http://service-inventory:8080`.

Desde la raíz:

```sh
make up                     # docker compose up -d --build --wait: todo healthy
make logs                   # logs JSON de los dos servicios
make demo-circuit-breaker   # Inventory caído -> OPEN -> HALF_OPEN -> CLOSED (~40 s)
make down
```

`make demo-circuit-breaker` (`scripts/demo-circuit-breaker.sh`) hace lo siguiente:

1. Detiene service-inventory y lanza 10 confirmaciones, que responden 503: el circuito se abre tras las primeras.
2. Muestra `/actuator/circuitbreakers` y que readiness sigue `UP`.
3. Vuelve a levantar Inventory y espera a que el circuito pase a HALF_OPEN (`INVENTORY_CB_WAIT`, 30 s por defecto).
4. Reintenta 3 órdenes pendientes (las llamadas de prueba de HALF_OPEN), que responden 200: el circuito queda CLOSED.

**Variables `DB_*` del shell.** El `.env` de la raíz (MySQL) y `order-service/.env` (PostgreSQL) usan los mismos nombres `DB_*`, y Compose da prioridad a las variables del shell. Si en el terminal se exportó `order-service/.env` para lanzar `spring-boot:run`, un `docker compose up` recrearía MySQL con el puerto, la base y el usuario de Order. Los targets `make up/down/logs` y la demo descartan esas variables. Si usas `docker compose` a mano, hazlo desde un terminal limpio o antepón `env -u DB_HOST -u DB_PORT -u DB_NAME -u DB_USER -u DB_PASSWORD`.

**Puerto 8081.** El servicio publica `127.0.0.1:${ORDER_PORT:-8081}`, el mismo puerto que usa `spring-boot:run`. Para usar uno de los dos, detén el otro, o cambia `ORDER_PORT`. El scrape de Prometheus encuentra Order en cualquiera de los dos casos.

## Kubernetes

Los manifiestos usan Kustomize y la misma estructura y estándares que Inventory (labels, `securityContext`, probes, recursos, HPA, PDB y rollout `maxUnavailable: 0`). Todo va en el namespace `geovannycode`:

```text
k8s/
  namespace/                  namespace, aplicado aparte (borrar las apps nunca borra las BD)
  infra/mysql/                MySQL de Inventory (solo local; en EKS, RDS)
  infra/postgres/             StatefulSet postgres:17.11 + Service headless + PVC + Secret + NetworkPolicy (solo local; en EKS, RDS)
  infra/kafka/                Kafka 4.3.1 KRaft de un nodo + Service headless + PVC + Job que crea orders.events.v1 (solo local; en EKS, MSK)
  inventory/{base,overlays}/  service-inventory
  order/base/                 ConfigMap, ServiceAccount, Job de migración, Deployment, Service, HPA, PDB
  order/overlays/minikube/    Secret de BD, NodePort 30081, imagePullPolicy Never, contexto local, CB wait 15 s
  order/overlays/eks/         placeholder con TODOs (ECR, RDS, MSK, Secrets Manager, IRSA, puerto de management)
  overlays/{minikube,eks}/    kustomization raíz que lo incluye todo (dry-run, diff, GitOps)
```

- **Configuración** (`order/base/config.env`, ConfigMap con hash):
  - `SPRING_PROFILES_ACTIVE=k8s`;
  - `DB_HOST=postgres`, `DB_PORT=5432` y `DB_NAME=orderdb`;
  - `INVENTORY_BASE_URL=http://service-inventory.geovannycode.svc.cluster.local/services-inventory`;
  - `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`;
  - `LIQUIBASE_CONTEXTS` (`prod`, que el overlay local cambia a `local`);
  - `INVENTORY_CB_WAIT`;
  - variables de trazas.
- **Secretos:** `DB_USER` y `DB_PASSWORD` llegan por `secretGenerator` desde `order/overlays/minikube/.env`, y los de PostgreSQL desde `infra/postgres/.env`. Ninguno se versiona: `make k8s-secrets` los genera a partir del `.env` de la raíz (`POSTGRES_*`).
- **Deployment:** contenedor en el puerto 8081 (`http`), usuario 10001, raíz de solo lectura y `/tmp` en `emptyDir` (lo necesitan Netty y zstd-jni).
  - Probes contra `/services-order/actuator/health/{liveness,readiness}`. Readiness incluye solo `readinessState` y `r2dbc`, así que ni el circuito abierto ni Kafka sacan el pod del balanceo.
  - `startupProbe` de hasta 120 s (60 × 2 s), frente a los ~3 s medidos.
  - `preStop` de 5 s más 20 s de apagado ordenado, dentro de los 30 s del grace period.
- **Service:** ClusterIP `service-order`, 80 → `http` (8081). Dentro del clúster: `http://service-order.geovannycode.svc.cluster.local/services-order`.
- **Selectores:** el Deployment, el Service y el PDB seleccionan `app.kubernetes.io/component: api`, para dejar fuera el pod del Job de migración.
- **HPA y PDB:** el HPA va de 1 a 4 réplicas al 70 % de CPU; el overlay `eks` sube el mínimo a 2 y cambia el PDB a `minAvailable: 1`.
- **Kafka:** el broker tiene la autocreación de topics desactivada, y el `NewTopic` de Order solo existe en `local`, `docker` y `test`. El topic lo crea el Job `kafka-topics` (idempotente, `--if-not-exists`), como un recurso de plataforma.

### Migraciones: Job de Liquibase ([ADR 0006](../docs/adr/0006-liquibase-on-kubernetes.md))

Los pods arrancan con `spring.liquibase.enabled=false` (`application-k8s.yml`). Las migraciones las aplica el Job `service-order-db-migration`, que usa **la misma imagen** de la app en modo "solo migrar":

- `SPRING_LIQUIBASE_ENABLED=true`;
- `-Dspring.context.exit=onRefresh`: Liquibase corre en el refresh y la JVM termina antes de levantar el servidor web, el relay o el productor de Kafka.

`make k8s-migrate` borra el Job anterior (los Jobs son inmutables), aplica el overlay sin el Deployment y espera a que termine. Después `make k8s-apps` despliega los servicios. Volver a ejecutarlo no aplica nada: `Database is up to date, no changesets to execute`. El ADR explica cómo revisar el SQL (`update-sql`) y cómo liberar un lock colgado (`release-locks`).

### NetworkPolicy

| Destino | Acepta tráfico de |
|---|---|
| `service-inventory` | pods de Order (`component: api`), el namespace `ingress-nginx` (fase conjunta) y el pod `k6-load` de `make k8s-load` |
| `postgres` | pods de Order (la app y el Job de migración) |
| `mysql` | pods de Inventory |

Docker Desktop crea las NetworkPolicy pero **no las aplica**, porque su CNI no lo soporta. Para verlas en acción hace falta un CNI con soporte, por ejemplo minikube con Calico:

```sh
minikube start --cni=calico --cpus=4 --memory=6g
make k8s-up
kubectl run probe -n geovannycode --rm -i --restart=Never --image=busybox:1.37 -- \
  wget -qO- -T 3 http://service-inventory/services-inventory/actuator/health   # bloqueado: timeout
```

Con las políticas activas, Postman ya no llega a Inventory por su NodePort. Hay que pasar por Order o usar `kubectl port-forward`, que no atraviesa las NetworkPolicy.

### Despliegue

```sh
make k8s-up CLUSTER=docker-desktop   # o sin CLUSTER para minikube
make k8s-validate k8s-lint           # dry-run en el servidor, kubeconform y kube-linter
kubectl get pods -n geovannycode     # 5 pods Running y Ready + 2 Jobs Completed
```

`make k8s-up` encadena estos pasos:

1. Genera los Secret, prepara el clúster (metrics-server) y construye las dos imágenes.
2. Aplica el namespace y la infraestructura (MySQL, PostgreSQL, Kafka), y espera sus rollouts y el Job del topic.
3. Ejecuta el Job de migración y espera a que termine.
4. Despliega Inventory y Order con `rollout restart` (el tag es fijo y `imagePullPolicy: Never`).
5. Muestra las URLs: con Docker Desktop, `http://localhost:30080` (Inventory) y `http://localhost:30081` (Order).

**Postman:** la carpeta *Order Service / k8s*, con el entorno `postman/order-k8s.postman_environment.json` (`domainOSk8s` y `domainISk8s`). Cubre actuator, crear, confirmar, el stock descontado en Inventory, los rechazos 409, la orden inexistente (404) y el listado. Para ver el evento:

```sh
kubectl exec -n geovannycode kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic orders.events.v1 --from-beginning --timeout-ms 5000 --formatter-property print.key=true
```

### Demo de resiliencia

```sh
O=http://localhost:30081/services-order
for i in 1 2 3; do curl -s -X POST $O/orders -H 'Content-Type: application/json' -d '{"codeProduct":"AC-1551","quantity":1}'; echo; done

kubectl scale deploy/service-inventory --replicas=0 -n geovannycode
curl -i -X PUT $O/orders/<id>                      # 503 + Retry-After: 15, para cada orden
curl -s $O/actuator/circuitbreakers                # "state":"OPEN"
curl -s $O/orders/<id>                             # sigue "pending"
kubectl get pods -n geovannycode -l app.kubernetes.io/name=service-order   # 1/1 Ready, sin reinicios

kubectl scale deploy/service-inventory --replicas=1 -n geovannycode
kubectl rollout status deploy/service-inventory -n geovannycode
# pasados 15 s (INVENTORY_CB_WAIT del overlay local) el circuito pasa a HALF_OPEN
curl -X PUT $O/orders/<id>                         # 200 completed, las mismas órdenes; tras 3 éxitos, CLOSED
```

El HPA no interfiere: con el Deployment a 0 réplicas, el HPA deja de escalar (`ScalingDisabled`) hasta que vuelve a 1.

Resultado medido (Docker Desktop, 2026-10-04):

- Con Inventory a 0 réplicas, las órdenes 7, 8 y 9 respondieron 503 con `Retry-After: 15`, el circuito pasó a OPEN y las tres siguieron `pending`.
- Al volver Inventory, las mismas tres respondieron 200 `completed` y el circuito volvió a CLOSED.
- El log de Order muestra `CLOSED -> OPEN`, `OPEN -> HALF_OPEN` y `HALF_OPEN -> CLOSED`.
- El pod de Order siguió 1/1 Ready y con 0 reinicios durante toda la demo.

## Tests

| Nivel | Clases | Qué demuestra |
|---|---|---|
| Unitario (Surefire, `*Test`) | `OrderServiceTest` | Un test por fila de la tabla de `confirm` (inexistente, completed sin llamar al gateway, canceled, éxito, rechazo con `cancelReason`, Inventory caído sin guardar nada, conflicto de versión con relectura y su límite), más `create`, `findById` y `findAll`. |
| Slice web | `OrdersApiTest` (`@WebFluxTest` + `@MockitoBean`) | Status, `Location`, `Retry-After`, `Content-Type` y cuerpo de cada endpoint; `ProblemDetail` de 400, 404, 409, 503 y 500; NDJSON y SSE. |
| Contrato del consumidor | `InventoryContractTest` | Las respuestas de WireMock (200, 404, 409 y 422, en `InventoryStubs`) y la petición que envía el gateway cumplen `contracts/services-inventory.yaml`. Si Inventory cambia su contrato, el build falla. |
| Observabilidad | `ObservabilityIT` (exportador de spans en memoria) | `/actuator/prometheus` contiene las métricas de Resilience4j de `inventory` y las de negocio tras confirmaciones completed, canceled y 503. Una traza con `traceparent` entrante llega a Inventory con tres spans `CLIENT` (dos 500 y un 200) y spans de R2DBC. El mensaje de Kafka lleva el `traceparent` de la confirmación. Con el circuito OPEN, readiness y health siguen `UP` y `/actuator/health/resilience` muestra `OPEN`. Están expuestos `retries` y `ratelimiters`, e `info` muestra `build`. |
| Arquitectura | `ArchitectureTest` (ArchUnit) | `api` no usa `infrastructure`, `domain` no depende de Spring, solo `infrastructure.inventory` usa WebClient, HTTP service clients y Resilience4j, y no hay ciclos. |
| Outbox (Failsafe) | `OutboxIT` (PostgreSQL y Kafka en Testcontainers) | Una confirmación produce exactamente un mensaje con key y headers correctos y deja `published_at`; un rechazo produce `OrderCanceled` con `cancelReason`; con Kafka pausado la confirmación responde 200, readiness sigue `UP` y el evento se publica al volver; si falla el guardado de la orden o el `INSERT` en la outbox, no queda ni evento ni cambio de estado; dos relays en paralelo no publican el mismo evento dos veces. |
| Integración (Failsafe, `*IT`) | `OrderApiIT`, `InventoryGatewayIT`, `OrderRepositoryIT` | HTTP real contra PostgreSQL 17.11 (Testcontainers 2), con WireMock como Inventory: flujo feliz, rechazo con orden `canceled` persistida, Inventory caído con 503 y orden `pending`, 20 PUT concurrentes sobre la misma orden (repetido 5 veces) y resiliencia del gateway. |
| Sistema (`-Psystem-tests`) | `OrderInventorySystemIT` | Order contra la imagen real `geovannycode/service-inventory` y MySQL: stock 10, 15 órdenes de 1 unidad confirmadas dos veces en paralelo. Resultado: 10 `completed`, 5 `canceled` por `INSUFFICIENT_STOCK` y stock final 0. |

Todos los contextos completos levantan PostgreSQL y Kafka (`TestcontainersConfiguration`, con los mismos digests que Compose), porque el relay corre en cada uno. Cada test es independiente: en `@BeforeEach` se vacía la tabla `order_shop`, se reinicia WireMock y se resetea el circuit breaker. No hay `Thread.sleep`: la espera de los tests asíncronos va con `StepVerifier` y timeouts.

Liquibase corre el changelog real con el contexto `test` (`src/test/resources/application-test.yml`), sin datos semilla.

```sh
./mvnw verify                    # unitarios + integración + JaCoCo (requiere Docker)
./mvnw verify -Psystem-tests     # además, Order contra la imagen real de Inventory
```

**Cobertura (JaCoCo 0.8.15).** Un solo informe para unitarios e integración, en `target/site/jacoco/index.html`. `verify` falla por debajo del 80 % de líneas o del 70 % de ramas. Se excluyen el código generado (`generated/**`), `OrderServiceApplication` y los records sin lógica (`InventoryClientProperties`, `GlobalExceptionHandler.ValidationError`).

**El test de sistema necesita la imagen de Inventory.** Si no existe, falla con el comando para construirla:

```sh
docker build --build-context contracts=contracts -t geovannycode/service-inventory:0.0.1-SNAPSHOT inventory-service
```

No se construye desde el test con `ImageFromDockerfile` porque el Dockerfile necesita BuildKit (contexto con nombre para el contrato y `--mount=type=cache`), y Testcontainers no lo soporta. Para usar otra etiqueta: `-Dinventory.image=...`.

## Verificación

```sh
./mvnw -q verify
curl http://localhost:8081/services-order/actuator/health
curl http://localhost:8081/services-order/actuator/health/liveness
curl http://localhost:8081/services-order/actuator/health/readiness
```

El servicio escucha en el puerto 8081 con el prefijo `/services-order`. Actuator expone solo `health` e `info`; `/services-order/actuator/env` responde 404.
