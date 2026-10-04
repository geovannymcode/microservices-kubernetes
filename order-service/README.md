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
- **Errores:** `ProblemDetail` con `type` `https://codearti.com/problems/<slug>`, `title`, `status`, `detail`, `instance`, `timestamp` y `errors[]` en validación; el mismo formato que Inventory. El JSON también es estricto: `quantity: 1.9` o `"2"` → 400.
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

## Tests

| Nivel | Clases | Qué demuestra |
|---|---|---|
| Unitario (Surefire, `*Test`) | `OrderServiceTest` | Un test por fila de la tabla de `confirm` (inexistente, completed sin llamar al gateway, canceled, éxito, rechazo con `cancelReason`, Inventory caído sin guardar nada, conflicto de versión con relectura y su límite), más `create`, `findById` y `findAll`. |
| Slice web | `OrdersApiTest` (`@WebFluxTest` + `@MockitoBean`) | Status, `Location`, `Retry-After`, `Content-Type` y cuerpo de cada endpoint; `ProblemDetail` de 400, 404, 409, 503 y 500; NDJSON y SSE. |
| Contrato del consumidor | `InventoryContractTest` | Las respuestas de WireMock (200, 404, 409 y 422, en `InventoryStubs`) y la petición que envía el gateway cumplen `contracts/services-inventory.yaml`. Si Inventory cambia su contrato, el build falla. |
| Arquitectura | `ArchitectureTest` (ArchUnit) | `api` no usa `infrastructure`, `domain` no depende de Spring, solo `infrastructure.inventory` usa WebClient, HTTP service clients y Resilience4j, y no hay ciclos. |
| Outbox (Failsafe) | `OutboxIT` (PostgreSQL y Kafka en Testcontainers) | Una confirmación produce exactamente un mensaje con key y headers correctos y deja `published_at`; un rechazo produce `OrderCanceled` con `cancelReason`; con Kafka pausado la confirmación responde 200, readiness sigue `UP` y el evento se publica al volver; si falla el guardado de la orden o el `INSERT` en la outbox, no queda ni evento ni cambio de estado; dos relays en paralelo no publican el mismo evento dos veces. |
| Integración (Failsafe, `*IT`) | `OrderApiIT`, `InventoryGatewayIT`, `OrderRepositoryIT` | HTTP real contra PostgreSQL 17.11 (Testcontainers 2), con WireMock como Inventory: flujo feliz, rechazo con orden `canceled` persistida, Inventory caído con 503 y orden `pending`, 20 PUT concurrentes sobre la misma orden (repetido 5 veces) y resiliencia del gateway. |
| Sistema (`-Psystem-tests`) | `OrderInventorySystemIT` | Order contra la imagen real `codearti/service-inventory` y MySQL: stock 10, 15 órdenes de 1 unidad confirmadas dos veces en paralelo. Resultado: 10 `completed`, 5 `canceled` por `INSUFFICIENT_STOCK` y stock final 0. |

Todos los contextos completos levantan PostgreSQL y Kafka (`TestcontainersConfiguration`, con los mismos digests que Compose), porque el relay corre en cada uno. Cada test es independiente: en `@BeforeEach` se vacía la tabla `order_shop`, se reinicia WireMock y se resetea el circuit breaker. No hay `Thread.sleep`: la espera de los tests asíncronos va con `StepVerifier` y timeouts.

Liquibase corre el changelog real con el contexto `test` (`src/test/resources/application-test.yml`), sin datos semilla.

```sh
./mvnw verify                    # unitarios + integración + JaCoCo (requiere Docker)
./mvnw verify -Psystem-tests     # además, Order contra la imagen real de Inventory
```

**Cobertura (JaCoCo 0.8.15).** Un solo informe para unitarios e integración, en `target/site/jacoco/index.html`. `verify` falla por debajo del 80 % de líneas o del 70 % de ramas. Se excluyen el código generado (`generated/**`), `OrderServiceApplication` y los records sin lógica (`InventoryClientProperties`, `GlobalExceptionHandler.ValidationError`).

**El test de sistema necesita la imagen de Inventory.** Si no existe, falla con el comando para construirla:

```sh
docker build --build-context contracts=contracts -t codearti/service-inventory:0.0.1-SNAPSHOT inventory-service
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
