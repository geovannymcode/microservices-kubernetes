# service-notify

Microservicio reactivo de notificaciones. Consume los eventos de órdenes que publica Order en Kafka (`orders.events.v1`), registra una notificación por evento en MongoDB, la envía por un canal y expone una API de solo lectura.

Estado: **fase 9**. Consume `orders.events.v1`, guarda una notificación por evento en MongoDB, la envía por el canal `log` o `webhook` y expone la API de solo lectura del contrato v2.0.0. Además publica métricas, trazas y logs, y corre en Compose.

## Requisitos

- JDK 25 y Maven 3.9 o superior (se usa el wrapper `./mvnw`, Maven 3.9.16).
- MongoDB 8.0 con un usuario de la aplicación en la base `db_notify`.

## Stack

Java 25, Spring Boot 4.1.1, WebFlux, Spring Data MongoDB reactivo (`spring-boot-starter-data-mongodb-reactive`), spring-kafka, Actuator, Bean Validation, Micrometer (Prometheus) y `spring-boot-starter-opentelemetry` (OTLP).

## Configuración

Todas las conexiones se leen de variables de entorno; no hay credenciales en el repositorio:

| Variable | Valor por defecto |
|---|---|
| `MONGO_HOST` / `MONGO_PORT` | `localhost` / `27017` |
| `MONGO_DB` | `db_notify` (también es la base de autenticación) |
| `MONGO_USER` / `MONGO_PASSWORD` | `notify` / (vacío) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `NOTIFY_CONSUMER_CONCURRENCY` | `3` (un hilo por partición de `orders.events.v1`) |
| `NOTIFY_PROCESSING_TIMEOUT` | `10s` (espera máxima del listener por evento) |
| `NOTIFICATION_SENDER_TYPE` | `log` (o `webhook`, ver [Canales de envío](#canales-de-envío)) |
| `NOTIFY_WEBHOOK_URL` | (vacío; obligatoria con `webhook`) |
| `NOTIFY_WEBHOOK_SECRET` | (vacío = sin firma) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` (trazas en `/v1/traces`, logs en `/v1/logs`) |
| `OTEL_LOGS_EXPORTER` | sin definir = logs solo en consola; `otlp` los envía también por OTLP |
| `TRACING_SAMPLING` | `1.0` |

Boot 4 movió las propiedades de conexión de `spring.data.mongodb.*` a `spring.mongodb.*`. Las antiguas están marcadas como obsoletas con nivel `error`, así que `application.yml` usa las nuevas.

## Base de datos (MongoDB 8.0)

El `docker-compose.yaml` de la raíz levanta `notify-mongo` (`mongo:8.0.32`, fijada por digest) en `127.0.0.1:27017`:

- **Volumen:** `mongo-data`, en la misma red que el resto de servicios.
- **Healthcheck:** `mongosh` con `db.adminCommand('ping')`, que no necesita autenticarse.
- **Usuario de la aplicación.** `MONGO_INITDB_ROOT_*` solo sirve para inicializar. `db-scripts/mongo-init.js` crea en `MONGO_DB` el usuario `MONGO_USER` con el rol `readWrite`, leyendo las credenciales de `process.env`. La aplicación conecta siempre con ese usuario, nunca como root.
- **Primer arranque.** Los scripts de `/docker-entrypoint-initdb.d` solo se ejecutan con el volumen vacío. Para cambiar la contraseña después, usa `db.changeUserPassword` o borra el volumen.

En el `.env` de la raíz hacen falta `MONGO_INITDB_ROOT_PASSWORD`, `MONGO_DB`, `MONGO_USER` y `MONGO_PASSWORD` (ver `.env.example`). Los tres últimos deben coincidir con los de `notify-service/.env`. Compose no arranca ningún servicio hasta que existan.

```sh
docker compose up -d mongodb      # desde la raíz
docker compose ps mongodb         # healthy
```

## Persistencia

- **Colección:** `notify_orders` en la base `db_notify`.
- **`NotificationDocument`:** un `record` inmutable. `markSent`, `markFailed` y `withAttempt` devuelven una copia.
  - `@CreatedDate` se rellena con la auditoría reactiva (`@EnableReactiveMongoAuditing`).
  - `@Version` hace que una segunda escritura sobre la misma versión falle con `OptimisticLockingFailureException`.
- **`status`** se guarda en minúscula (`pending`, `sent`, `failed`) mediante converters propios (`NotifyStatusConverters`). Sin ellos, Spring Data guardaría el nombre del enum.
- **`NotificationRepository`** ofrece `findByEventId`. **`NotificationQueryRepository`** lista con filtros opcionales `orderId` y `status`, de más reciente a más antigua (`createdAt` y luego `_id`) y con límite. Usa `ReactiveMongoTemplate` y `Criteria`, porque con métodos derivados haría falta uno por cada combinación de filtros.

### Índices

`spring.data.mongodb.auto-index-creation` está desactivado: los índices se crean de forma explícita en `MongoIndexInitializer`.

| Índice | Campos | Para qué |
|---|---|---|
| `ux_event_id` (único) | `eventId` | Un evento repetido (entrega al menos una vez) no puede crear dos notificaciones. |
| `ix_order_id` | `orderId` | Filtrar por orden. |
| `ix_status_created_at` | `status`, `createdAt` desc | Filtrar por estado ordenando por fecha. |

- **Creación no bloqueante:** se lanza al arrancar (`ApplicationStartedEvent`) y es idempotente. Arrancar dos veces no falla, porque `createIndex` con la misma definición no hace nada.
- **Readiness:** el inicializador es también el indicador de salud `mongoIndexes`, que forma parte de readiness (`readinessState,mongo,mongoIndexes`). El servicio no recibe tráfico hasta que existe el índice único. El listener de Kafka podrá esperar a `MongoIndexInitializer.ready()`.

Para comprobar los índices:

```sh
docker exec -it notify-mongo mongosh -u notify -p --authenticationDatabase db_notify db_notify \
  --eval 'db.notify_orders.getIndexes()'
```

## Contrato y API (API-first)

El contrato es [`contracts/services-notify.yaml`](../contracts/services-notify.yaml) (v2.0.0, OpenAPI 3.0.3), en la raíz del repo. No hay copia en `src/main/resources`. `npx @redocly/cli lint` lo valida sin errores; solo avisa de que `info` no tiene `license`.

`openapi-generator-maven-plugin` 7.25.0 (ejecución `notify-server`) genera el código en `target/` con las mismas opciones que Order (ver su ADR 0003). Ese código no se versiona ni se edita:

- **Interfaces:** `NotifyApi` (controlador), `NotifyApiDelegate` y `NotifyApiController`, en `com.geovannycode.notify_service.generated.api`.
- **DTO:** `NotifyResponse`, `NotifyStatus`, `ProblemDetail` y `ProblemDetailErrorsInner`, en `...generated.dto`.
- **Opciones:** `reactive`, `delegatePattern`, `useSpringBoot4`, `useJackson3`, `useTags`, `useResponseEntity`, `useBeanValidation` y `openApiNullable=false`. Con `requestMappingMode=none`, el prefijo `/services-notify` lo pone `spring.webflux.base-path` y no el controlador.
- **`EnumConverterConfiguration`** (también generado) convierte `?status=sent` con `NotifyStatus.fromValue`. Sin él, Spring usaría `valueOf` y los valores en minúscula del contrato darían 400.
- **Única dependencia añadida** para el código generado: `swagger-annotations-jakarta`, por las anotaciones `@Operation` y `@Schema`.

**Endpoints** (solo lectura; no hay POST, PUT ni DELETE):

| Operación | Respuesta |
|---|---|
| `GET /notify?orderId&status&limit` | Notificaciones de la más reciente a la más antigua (`createdAt`, luego `_id`), con `limit` por defecto 100 (de 1 a 500). Admite JSON, NDJSON (`application/x-ndjson`) y SSE (`text/event-stream`, un evento por notificación con `id` = id de la notificación y `event: notify`). |
| `GET /notify/{notifyId}` | La notificación. Si no existe, siempre 404 `notify-not-found`, nunca un 200 vacío. |

- **`NotifyApiDelegateImpl`** es solo un adaptador HTTP: el controlador generado enruta y valida, y `NotificationService.findAll` y `findById` responden.
- **SSE.** El generador da una sola firma para los tres media types. Con SSE, el delegate devuelve `ServerSentEvent` dentro del mismo `Flux`, como en Order.
- **`NotifyApiTest`** comprueba además por reflexión que el delegate sobrescribe todas las operaciones generadas (`skipDefaultInterface` debe seguir en `false` con `delegatePattern`).

**Errores** (`GlobalExceptionHandler`, `ProblemDetail` con `type`, `title`, `status`, `detail`, `instance`, `timestamp` y `errors[]`, igual que Inventory y Order):

| Caso | Respuesta |
|---|---|
| Notificación inexistente | 404 `notify-not-found` |
| `limit` fuera de 1-500, `orderId` < 1, `notifyId` que no tiene 24 caracteres hexadecimales | 400 `validation-error` con `errors[]` y mensajes en español |
| `status` desconocido u otro parámetro que no se puede convertir | 400 `invalid-request` |
| Cualquier otro error | 500 `internal-error`, registrado en ERROR, sin exponer el mensaje interno |

La validación la hace el controlador generado (`@Validated`). Esa validación lanza `ConstraintViolationException`, que el handler convierte en 400.

**Swagger UI** está en `/services-notify/swagger-ui.html` y muestra el contrato estático. `maven-resources-plugin` copia `contracts/services-notify.yaml` al jar (`/services-notify/openapi/services-notify.yaml`), y el escaneo por reflexión de springdoc está vacío. Es lo mismo que en Order (springdoc 3.1.1).

**CORS**, solo en el perfil `local` y solo para `GET`: permite los visores de `web/` (`stream-viewer.html` para NDJSON y `stream-event-viewer.html` para SSE). Los visores tienen un selector de recurso (Inventario o Notificaciones); con Notificaciones apuntan a `http://localhost:8082/services-notify/notify`.

```sh
cd web && python3 -m http.server 8099     # http://localhost:8099/stream-viewer.html -> Recurso: Notificaciones
```

**`NotifyMapper`** (MapStruct 1.6.3, `componentModel = "spring"`) está en `application`, como en Order: `NotificationService` devuelve `NotifyResponse`, y con el mapper en `api` habría un ciclo entre `api` y `application`. Convierte `NotificationDocument` en `NotifyResponse`:

- `Instant` → `OffsetDateTime` en UTC;
- `eventId` → `UUID`;
- `eventType`, `channel` y `status` pasan por su valor en el contrato (`fromValue`), nunca por el nombre de la constante. Un valor desconocido falla en lugar de mapearse a `null`.

```sh
curl -s "http://localhost:8082/services-notify/notify?orderId=42"
curl -i http://localhost:8082/services-notify/notify/000000000000000000000000   # 404 notify-not-found
curl -i http://localhost:8082/services-notify/notify/abc                        # 400 validation-error
curl -N -H "Accept: text/event-stream" http://localhost:8082/services-notify/notify
```

## Consumo de eventos (Kafka)

Order publica en `orders.events.v1` (fase 9 de Order; contrato en [`contracts/events/order-events.yaml`](../contracts/events/order-events.yaml)):

- **key:** el id de la orden;
- **headers:** `eventType`, `eventId` y `traceparent`;
- **valor:** JSON en texto plano (`StringSerializer`), sin headers de tipo de Spring. Lo genera un campo JSONB, así que el orden de las claves varía y no se depende de él.

**Consumidor.** Grupo `service-notify`, `auto-offset-reset: earliest`, sin auto-commit, `ack-mode: record` y `concurrency: 3`.

- La key se lee con `StringDeserializer`.
- El valor se lee con `ErrorHandlingDeserializer` sobre `JacksonJsonDeserializer` (Jackson 3; `JsonDeserializer` es el de Jackson 2 y está obsoleto). El tipo es fijo, `OrderEvent`, y no se usan los headers de tipo.
- Los campos desconocidos se ignoran, para admitir cambios compatibles de la versión 1.

**`OrderEventListener`** valida el evento (Jakarta Validation) y llama a `NotificationService.handle`. Espera el resultado con `block(processing-timeout)`, que es la única llamada bloqueante del servicio: corre en el hilo del consumidor de Kafka ([ADR 0007](../docs/adr/0007-kafka-consumer-model.md)). El offset se confirma después de guardar y enviar.

| Caso | Resultado |
|---|---|
| No se puede deserializar o no cumple el formato | `orders.events.v1.dlt`, sin reintentos |
| `eventType` desconocido | Se ignora con WARN (no es un error ni va a la DLT) |
| Ya existe una notificación SENT con ese `eventId` | Se ignora (duplicado) |
| No existe | Se inserta PENDING, se envía y queda SENT (`attempts`, `sentAt`) |
| Existe en PENDING o FAILED (reentrega) | Se reenvía y queda SENT |
| El envío falla | Se cuenta el intento, sigue PENDING y Kafka reintenta. Al agotar los intentos: FAILED y DLT |
| MongoDB no responde (timeout de procesamiento o `DataAccessResourceFailureException`) | Kafka reintenta sin límite de intentos, con el mismo backoff. No va a la DLT: el evento no tiene la culpa |

- **Sin duplicados.** Lo garantiza el índice único de `eventId`: el servicio inserta y, si recibe `DuplicateKeyException`, lee la notificación existente. No hace "consultar y luego insertar". Antes de procesar espera a que exista el índice (`MongoIndexInitializer.ready()`).
- **Reintentos.** `DefaultErrorHandler` con backoff exponencial: 3 entregas en total, con esperas de 1 s y 2 s y un tope de 4 s (`notify.retry.*`). En la última, el listener marca la notificación FAILED y el `DeadLetterPublishingRecoverer` publica el evento en `orders.events.v1.dlt`, en la misma partición y con los headers `kafka_dlt-exception-*`.
- **MongoDB caído.** Si el fallo es de almacenamiento (`processing-timeout` agotado o `DataAccessResourceFailureException`, y no un fallo del canal), el `DefaultErrorHandler` cambia a un backoff exponencial sin límite de intentos (`setBackOffFunction`). La partición espera, conservando el orden, y el evento se procesa cuando MongoDB vuelve, en vez de acabar en la DLT. Cada intento dura como mucho `processing-timeout` más el tope del backoff, muy por debajo de `max.poll.interval.ms`, así que el grupo no expulsa al consumidor. Mientras tanto, la readiness (`mongo`) marca el servicio como no listo.
- **Topic DLT.** Lo crea un `NewTopic` con 3 particiones, solo en los perfiles `local`, `docker` y `test`.
- **Mensajes:**
  - completada: `La orden {orderId} ({codeProduct} x{quantity}) fue completada.`
  - cancelada: `... fue cancelada: {motivo}.`, donde el motivo es `el producto no existe` o `no hay stock suficiente`.
- **Canal.** El puerto `NotificationSender` recibe un `NotificationMessage` del dominio, no el documento de Mongo, para no crear un ciclo entre el dominio y la persistencia. Los canales se describen en la sección siguiente.
- **Rechazo del receptor.** Si el canal lanza `NotificationRejectedException` (un webhook que responde 4xx), el evento no se reintenta: la notificación queda FAILED en el primer intento y el evento va a la DLT.

Para ver el consumidor con el sistema completo (Kafka, Order e Inventory de Compose):

```sh
docker exec order-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group service-notify
docker exec -it notify-mongo mongosh -u notify -p --authenticationDatabase db_notify db_notify \
  --eval 'db.notify_orders.find({}, {_id:0, orderId:1, eventType:1, status:1, attempts:1, message:1}).sort({createdAt:-1}).limit(5)'
```

## Canales de envío

Solo hay un `NotificationSender` activo, elegido con `notification.sender.type` (`NOTIFICATION_SENDER_TYPE`). Las propiedades se validan al arrancar (`NotificationSenderProperties`): un tipo desconocido, o `webhook` sin una URL http(s) válida, impiden que la aplicación arranque y el error dice qué falta:

```
Property: notification.sender.webhookUrlValidForType
Reason: con notification.sender.type=webhook hay que definir notification.sender.webhook.url (variable NOTIFY_WEBHOOK_URL) con una URL absoluta http o https
```

### `log` (por defecto)

`LogNotificationSender` escribe la notificación en el log, en INFO.

### `webhook`

`WebhookNotificationSender` hace un `POST` JSON a `NOTIFY_WEBHOOK_URL`:

```json
{"notificationId":"6ac37e98d812e36fa6f2fe30","eventId":"49023826-ab3d-48d2-ba9b-77d4c31f1094","eventType":"OrderCompleted",
 "orderId":76,"codeProduct":"AC-1551","quantity":1,"cancelReason":null,
 "message":"La orden 76 (AC-1551 x1) fue completada.","createdAt":"2026-10-05T10:40:24.413Z"}
```

- **Headers.** `Idempotency-Key` lleva el `eventId`: un reintento reenvía la misma notificación y el receptor puede descartar las repetidas. Si hay `NOTIFY_WEBHOOK_SECRET`, `X-Signature` lleva el HMAC-SHA256 en hexadecimal de los bytes exactos del cuerpo.
- **Cliente.** Se construye con el `WebClient.Builder` de Boot, para que se apliquen sus customizers (trazas y métricas). Los timeouts van en el `HttpClient` de Reactor Netty: conexión `1s` y respuesta `3s` (`notification.sender.webhook.connect-timeout` y `response-timeout`).

| Respuesta | Resultado |
|---|---|
| 2xx | Enviada: SENT |
| 4xx (salvo 408 y 429) | `NotificationRejectedException`: FAILED en el primer intento y DLT, sin reintentos |
| 5xx, 408, 429, timeout o error de conexión | `NotificationDeliveryException`: Kafka reintenta (3 intentos), después FAILED y DLT |

- **Sin Resilience4j.** El canal no reintenta por su cuenta. Los reintentos los hace el consumidor de Kafka, y dos capas de reintentos multiplicarían los intentos (3 × 3).
- **Sin secretos en los logs.** Ni el secret ni la URL completa aparecen en los logs ni en los errores (tampoco en los headers de la DLT). La URL puede llevar un token en la ruta o en la query, así que al arrancar solo se registra el destino (`http://localhost:8086 (ruta y query omitidas)`). Los errores de conexión llevan solo la causa raíz (`ConnectException: Connection refused`), porque los mensajes de WebClient incluyen la URL.

**Probarlo con el receptor local.** Desde la raíz del repo levanta el receptor (`mendhak/http-https-echo`, en el profile `tools`). Responde 200 y escribe en su log cada petición con sus headers y su cuerpo:

```sh
docker compose --profile tools up -d webhook-receiver
```

Arranca el servicio con el canal webhook (desde `notify-service`):

```sh
set -a; . ./.env; set +a
NOTIFICATION_SENDER_TYPE=webhook NOTIFY_WEBHOOK_URL=http://localhost:8086/notifications NOTIFY_WEBHOOK_SECRET=demo-secret \
  ./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

Confirma una orden en Order y comprueba el resultado:

```sh
docker logs -f notify-webhook-receiver                                   # el POST con Idempotency-Key y X-Signature
curl -s "http://localhost:8082/services-notify/notify?orderId=<id>"      # status sent, channel webhook
```

Para verificar la firma, toma el `body` que imprime el receptor:

```sh
printf '%s' '<body>' | openssl dgst -sha256 -hmac demo-secret
```

**Receptor caído.** Con `docker stop notify-webhook-receiver` y otra orden confirmada, la notificación queda `failed` con `attempts: 3` y el evento aparece en `orders.events.v1.dlt` (por ejemplo, en Kafka UI: `docker compose --profile tools up -d kafka-ui`, http://localhost:8085).

**Con un servicio público de prueba.** Funciona igual con una URL de https://webhook.site, que da una URL única y muestra cada petición en el navegador:

```sh
NOTIFY_WEBHOOK_URL=https://webhook.site/<tu-uuid>
```

Ahí salen datos reales de las órdenes hacia un tercero: úsalo solo con datos de prueba.

## Arranque local

```sh
cp .env.example .env          # completa MONGO_PASSWORD
set -a; . ./.env; set +a
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

El servicio escucha en el puerto 8082 con el prefijo `/services-notify`:

```sh
curl http://localhost:8082/services-notify/actuator/health
curl http://localhost:8082/services-notify/actuator/health/liveness
curl http://localhost:8082/services-notify/actuator/health/readiness
```

Actuator expone `health`, `info`, `prometheus` y `metrics` en el mismo puerto, bajo `/services-notify`. `/services-notify/actuator/env` responde 404.

**En Compose**, desde la raíz del repo. El perfil `docker` usa `mongodb` y `kafka:19092` (el listener interno de Kafka en Compose; `localhost:9092` es el externo) y escribe los logs en JSON ECS:

```sh
make up                     # los tres servicios
make up-observability       # más Grafana, Loki, Tempo, Prometheus y kafka-exporter
docker compose logs -f service-notify
```

## Contenedor

[`Dockerfile`](Dockerfile) usa la misma estrategia que Order e Inventory ([ADR 0002](../docs/adr/0002-container-image.md)):

- **Compilación:** con el wrapper, sin tests (el CI ejecuta `verify` aparte). `contracts/services-notify.yaml` llega como contexto de build con nombre, no se copia al módulo.
- **JRE mínima con `jlink`:** el mismo conjunto de módulos que Order (22 con sus dependencias). `java.security.sasl` sirve a Kafka y al SCRAM de MongoDB.
- **Base:** `gcr.io/distroless/cc-debian13`, sin shell, sin JDK y sin fuentes; `.dockerignore` excluye `.env`. Corre con el usuario `10001:10001`.
- **Capas:** `dependencies`, `spring-boot-loader`, `snapshot-dependencies` y `application`, de la menos a la más cambiante.
- **Arranque:** `JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"` y la caché AOT de Java 25.

**Entrenamiento de la caché AOT.** Se ejecuta `-Dspring.context.exit=onRefresh`, que refresca el contexto y sale antes de arrancar los beans con ciclo de vida, así que no necesita MongoDB ni Kafka:

- **Listeners:** los contenedores de los `@KafkaListener` son `SmartLifecycle` y no llegan a arrancar.
- **Índices:** `MongoIndexInitializer` los crea con `ApplicationStartedEvent`, que tampoco llega a emitirse.
- **Topic de la DLT:** sin perfil activo no hay `NewTopic` que crear.
- **MongoDB:** el `MongoClient` reactivo no abre conexiones de trabajo hasta el primer uso. Su hilo de monitorización sí intenta conectar con `localhost:27017` y lo registra en INFO, pero no afecta a la caché.
- **Exportador de trazas:** `OTEL_TRACES_EXPORTER=none` en esa etapa evita el exportador OTLP que BuildKit inyecta en CI, el mismo problema que tuvo Order.

No hizo falta un perfil especial para desactivar nada.

Desde la raíz del repo:

```sh
docker build --build-context contracts=contracts -t geovannycode/service-notify:0.0.1-SNAPSHOT notify-service
```

Comprobaciones:

- **Tamaño:** 289 MB, en la línea de Order e Inventory.
- **Usuario:** `docker image inspect -f '{{.Config.User}}' <img>` muestra `10001:10001`, y `docker top service-notify -eo uid,pid,args` muestra el proceso con UID 10001. `docker run --rm <img> id` no sirve: la imagen no tiene `id` ni shell, y el entrypoint es `java -jar`, así que `id` llegaría a la aplicación como argumento.
- **Trivy:** `trivy image --severity HIGH,CRITICAL --ignore-unfixed` da 0 vulnerabilidades. El primer escaneo encontró 10, de Jackson 3.1.5 y de Jackson 2.21.5 (el que trae springdoc). Faltaba la misma sobrescritura de versiones que ya tenían Order e Inventory (`jackson-bom.version` 3.1.7 y `jackson-2-bom.version` 2.21.7).
- **CI:** [`.github/workflows/notify-ci.yml`](../.github/workflows/notify-ci.yml) hace lo mismo que el de Order: `verify` (tests y JaCoCo), y después build, Trivy y push a GHCR solo en `main`. Se dispara con cambios en `notify-service/**`, `contracts/**` o el propio workflow.

En la imagen, `/actuator/info` muestra `build` pero no `git`, porque el contexto de build no incluye `.git` (igual que en Order).

## Observabilidad

Usa las mismas decisiones que Order e Inventory: Prometheus por scrape, trazas por OTLP, logs ECS en `docker` y `k8s`, y `build-info` y `git` en `/info`. No hay Logstash ni `logstash-logback-encoder`.

### Health

- **Readiness:** `readinessState`, `mongo` y `mongoIndexes`.
- **Kafka fuera de la readiness, a propósito:** con Kafka caído, la API de consulta sigue respondiendo y el consumidor se reconecta solo. Sacar el pod del balanceo o reiniciarlo no devuelve Kafka.

Comprobado en Compose con `docker stop order-kafka`: readiness `UP`, `GET /notify` 200 y el contenedor `healthy`. Al volver Kafka, la siguiente orden se notifica sin reiniciar nada.

### Métricas (`/actuator/prometheus`)

Todas llevan el tag `application=service-notify`.

| Métrica | Tags | Qué mide |
|---|---|---|
| `notifications_processed_total` | `eventType`, `result` (`sent`, `duplicate`, `ignored`, `failed`) | Cada resultado una vez. Un `eventType` desconocido se etiqueta como `other` para acotar las series |
| `notifications_delivery_seconds` | `channel`, `outcome` (`success`, `error`) | Cada intento en el canal, con histograma |
| `notifications_dlt_total` | `reason` (`invalid`, `rejected`, `exhausted`) | Registros publicados en `orders.events.v1.dlt`. Solo cuenta si la publicación tuvo éxito, y se registra a 0 al arrancar |
| `kafka_consumer_fetch_manager_records_lag_max`, `..._records_lag` | `client_id` (y `topic`, `partition`) | Lag visto por cada consumidor (`KafkaClientMetrics`). La serie por partición aparece en el siguiente refresco del binder (60 s) |
| `spring_kafka_listener_seconds` | `name`, `result` | Tiempo del listener por registro (`spring.kafka.listener.observation-enabled`), con histograma |
| `kafka_consumergroup_lag` | `consumergroup`, `topic`, `partition` | Lag del grupo según el broker (`kafka-exporter`). Sigue midiendo con Notification detenido, cuando el lag del cliente no tiene quien lo publique |

### Trazas

- **Kafka:** con la observación activada en el listener, el span `orders.events.v1 process` es hijo del `traceparent` que inyecta el productor de Order.
- **Outbox de Order:** el relay ya conservaba el contexto, porque guarda el `traceparent` en la fila de la outbox y publica dentro de un span hijo (`OutboxTracing`). No hizo falta cambiar Order.
- **MongoDB:** Boot solo autoconfigura sus métricas. `MongoObservationCommandListener` y el `ContextProvider` reactivo (`MongoObservationConfiguration`) añaden un span por comando.
- **Webhook:** el `WebClient` del canal sale del builder de Boot, así que añade su span y propaga `traceparent` al receptor.

Traza comprobada en Tempo con el sistema completo:

```
service-order      SERVER    http put /orders/{orderId}
service-order      CLIENT    http put
service-inventory  SERVER    http put /inventories/{productId}
service-inventory  CLIENT    query (x3)
service-order      INTERNAL  outbox relay orders.events.v1
service-order      PRODUCER  orders.events.v1 send
service-notify     CONSUMER  orders.events.v1 process
service-notify     CLIENT    notify_orders.insert
service-notify     CLIENT    http post              (canal webhook)
service-notify     CLIENT    notify_orders.update
```

Para encontrarla, toma el `trace.id` de una línea de `docker compose logs service-notify` y búscalo en Grafana (Explore → Tempo).

### Logs

- **`trace.id` y `span.id`:** cada línea los lleva.
- **`eventId` y `orderId`:** cada línea del consumidor los lleva como campos propios. `OrderEventListener` los pone en el MDC y, como `Slf4jThreadLocalAccessor` los registra en context-propagation, siguen a la cadena reactiva en los hilos de Netty y del driver de MongoDB.
- **Perfiles `docker` y `k8s`:** JSON ECS en stdout, por ejemplo:

```json
{"@timestamp":"…","log":{"level":"INFO","logger":"…LogNotificationSender"},"process":{"thread":{"name":"multiThreadIoEventLoopGroup-3-3"}},
 "service":{"name":"service-notify","version":"0.0.1-SNAPSHOT","environment":"docker"},"message":"Notificación enviada: orderId=78, …",
 "trace.id":"59fa14dd46ba4643700e7f32c0e91f5d","span.id":"d19978f7015e0759","eventId":"2514c6fb-…","orderId":"78"}
```

- **OTLP:** `OpenTelemetryLogsConfiguration` conecta Logback al SDK de OpenTelemetry por código, sin `logback-spring.xml`, y Boot sigue controlando la consola. Con `OTEL_LOGS_EXPORTER=otlp` (Compose) los logs llegan a Loki con `trace_id`, y `eventId` y `orderId` como metadatos estructurados. En local está apagado, porque no hay colector.

### Dashboard

[`observability/dashboards/notify-consumer.json`](../observability/dashboards/notify-consumer.json) está en Grafana, en la carpeta geovannycode → *Notification — Consumidor de eventos de órdenes*, y muestra:

- el lag del grupo (broker y cliente);
- los eventos procesados por resultado;
- el tiempo de entrega por canal (p50 y p95, y errores);
- los mensajes en la DLT por motivo;
- el tiempo del listener.

Prueba del lag con todo en Compose: `docker stop service-notify`, confirma 20 órdenes y el lag sube a 20 (9, 6 y 5 por partición). Tras `docker start service-notify`, baja a 0 en unos 4 s y las 20 órdenes tienen su notificación `sent`.

## Verificación

```sh
./mvnw -q verify
```

`verify` aplica JaCoCo a los tests unitarios y de integración juntos, con un mínimo del 80 % de líneas y el 70 % de ramas. Excluye el código generado (`generated.**`), la clase main y los records sin lógica. El informe queda en `target/site/jacoco/index.html`.

- **Unitarios** (Surefire, `*Test`):
  - `contextLoads`;
  - `NotifyMapperTest` (todos los campos, fechas en UTC, enums por su valor y rechazo de valores desconocidos);
  - `NotifyApiTest`, slice web con el servicio simulado (`@MockitoBean`), que comprueba:
    - status, `Content-Type` y cuerpo de JSON, NDJSON y SSE;
    - el paso de filtros y límite;
    - los `ProblemDetail` 400, 404 y 500;
  - `NotificationServiceTest` (Mockito + StepVerifier): una prueba por fila de la tabla de decisión, más los mensajes, el rechazo sin envolver, `markFailed`, la espera al índice y las consultas (incluido el 404);
  - `NotificationSenderConfigurationTest` (`ApplicationContextRunner`, sin contenedores): `log` por defecto, `webhook` sustituye a `log` (un solo `NotificationSender`), no arranca sin URL, con una URL que no es http(s) ni con un tipo desconocido, y el `toString` de la configuración no muestra la URL ni el secret;
  - `OrderEventContractTest`: valida contra los esquemas de payload de [`contracts/events/order-events.yaml`](../contracts/events/order-events.yaml) (JSON Schema draft 7, con `json-schema-validator` 3.x sobre Jackson 3) lo siguiente:
    - los eventos que publican los tests (`OrderEventsKafka.event`);
    - los ejemplos del contrato;
    - un `OrderCompleted` tal como lo serializa Order, con `cancelReason: null`.

    También comprueba que los ejemplos se deserializan en `OrderEvent` y pasan su validación, y que el esquema rechaza un evento sin campos obligatorios, un `OrderCanceled` sin motivo y un `status` que no corresponde al tipo. Si Order cambia el evento y su contrato, este test falla;
  - `KafkaConsumerConfigurationTest`: qué fallos esperan a MongoDB sin límite y cuáles usan los 3 intentos, y el motivo con el que se cuenta cada registro en la DLT;
  - `ArchitectureTest` (ArchUnit):
    - `api` no accede a `infrastructure`;
    - `domain` no depende de Spring ni de Kafka;
    - solo `OrderEventListener` bloquea (`block*`, `toIterable`, `toStream` sobre `Mono` o `Flux`), con una comprobación de que la regla no es vacía;
    - Kafka solo en `messaging` y WebClient solo en `sender`;
    - no hay ciclos entre paquetes.
- **Integración** (Failsafe, `*IT`): `NotificationRepositoryIT`. Corre contra `mongo:8.0.32` en Testcontainers 2 (`@ServiceConnection`, con el mismo digest que Compose) y cubre:
  - el id, `createdAt` y `version` asignados al guardar;
  - `findByEventId`;
  - el `eventId` duplicado (`DuplicateKeyException`);
  - los filtros con su orden y su límite;
  - el `status` en minúscula, leído del documento crudo;
  - el bloqueo optimista;
  - la idempotencia de los índices.
- **Integración** (Failsafe, `*IT`): `NotifyApiIT` hace HTTP real contra MongoDB y comprueba el orden, los filtros `orderId` y `status`, el límite, el 404 y el 400, un evento SSE por notificación y Swagger UI.
- **Integración** (Failsafe, `*IT`): `OrderEventListenerIT`, con Kafka y MongoDB en Testcontainers y un canal de prueba que falla bajo demanda. Comprueba lo siguiente:
  - completada → SENT con su mensaje;
  - cancelada → mensaje con el motivo;
  - el mismo evento dos veces → una sola notificación;
  - JSON inválido → DLT, y el consumidor sigue con el siguiente;
  - evento sin campos obligatorios → DLT sin reintentos;
  - `eventType` desconocido → se ignora, sin DLT;
  - canal que falla siempre → FAILED con `attempts = 3` y en la DLT;
  - canal que falla una vez → SENT con `attempts = 2`;
  - canal que rechaza → FAILED con `attempts = 1` y en la DLT, sin reintentos.
- **Integración** (Failsafe, `*IT`): `WebhookNotificationSenderIT`, con WireMock como receptor (la URL lleva un token en la query) y Kafka y MongoDB en Testcontainers:
  - 200 → éxito, con el cuerpo, `Idempotency-Key` y `X-Signature` recalculada sobre los bytes recibidos;
  - 500 y 429 → reintentable; 400 → rechazo, no reintentable;
  - respuesta más lenta que el timeout → reintentable;
  - receptor inalcanzable → reintentable, y el error no contiene ni el token ni la ruta;
  - de punta a punta: 500, 500, 200 → SENT con `attempts = 3`, canal `webhook` y tres peticiones con la misma `Idempotency-Key`;
  - de punta a punta: 422 → FAILED con `attempts = 1`, en la DLT y sin el token en los headers.
- **Integración** (Failsafe, `*IT`): `ObservabilityIT`, con Kafka y MongoDB en Testcontainers y los spans en un exportador en memoria. Comprueba:
  - que `/actuator/prometheus` contiene las métricas de negocio tras procesar eventos (`sent`, `duplicate`, `ignored`, la DLT `invalid` y el histograma de entrega), el lag del consumidor y el timer del listener;
  - que un `traceparent` en el registro de Kafka continúa en el span de consumo y en los spans `insert` y `update` de MongoDB;
  - que la línea "Notificación enviada", escrita desde un hilo del driver de MongoDB, lleva `trace.id`, `eventId` y `orderId` en JSON ECS;
  - que `/info` expone `build`.
- **Integración** (Failsafe, `*IT`): `OrderEventDeliveryGuaranteesIT`, con Kafka y MongoDB en Testcontainers, su propio consumer group y un `processing-timeout` de 1 s:
  - **orden por partición:** 5 eventos de la misma orden, con el primero más lento, se entregan en el orden en que se publicaron;
  - **concurrencia:** el mismo evento 10 veces, repartido en las 3 particiones (3 hilos a la vez) y con el canal lento para forzar la carrera, da una sola notificación SENT y nada en la DLT. El canal recibe más de un intento (al menos una vez), por eso el webhook envía `Idempotency-Key`;
  - **rebalanceo:** con 50 eventos, se para el contenedor del listener (`KafkaListenerEndpointRegistry`) a mitad del lote y se vuelve a arrancar. Al final hay exactamente 50 notificaciones y ninguna duplicada;
  - **MongoDB no disponible:** se pausa el contenedor de MongoDB y el evento falla más veces que el límite normal, sin ir a la DLT. Al reanudarlo, queda SENT con una sola notificación.

Las clases que levantan un contexto completo limpian la colección antes de cada test. Ninguna usa `Thread.sleep` (solo Awaitility) y los dobles de Spring se declaran con `@MockitoBean` o `@TestBean`.

`./mvnw verify` necesita Docker.

### Test de sistema

```sh
./mvnw verify -Psystem-tests
```

`OrderToNotifySystemIT` queda fuera del `verify` normal. Levanta MySQL, PostgreSQL, MongoDB, Kafka, Inventory y Order en Testcontainers (con los digests de Compose), y Notification corre en la JVM del test. Comprueba lo siguiente:

- una orden confirmada con stock termina en una notificación `sent` de tipo `OrderCompleted`;
- una orden de un producto inexistente (409 en Order) termina en `OrderCanceled` con `PRODUCT_NOT_FOUND`;
- confirmar dos veces la misma orden deja una sola notificación, y se mantiene así durante 5 s (`await().during`).

Las imágenes de Inventory y Order deben existir; sus Dockerfiles necesitan BuildKit, así que no se construyen desde el test. Desde la raíz del repo:

```sh
docker build --build-context contracts=contracts -t geovannycode/service-inventory:0.0.1-SNAPSHOT inventory-service
docker build --build-context contracts=contracts -t geovannycode/service-order:0.0.1-SNAPSHOT order-service
```

Se pueden usar otras etiquetas con `-Dinventory.image=...` y `-Dorder.image=...`.

**Puerto aleatorio en loopback.** En el perfil `test`, el servidor escucha en `127.0.0.1` (`server.address`) y los tests llaman a `127.0.0.1`, no a `localhost`. En macOS, un bind al comodín (`*:puerto`) puede convivir con otro proceso que ya escucha en `127.0.0.1` con el mismo puerto, y entonces las peticiones a `localhost` llegan a ese proceso. Pasó con VS Code, que respondía 401.

## Kubernetes local — fase 10

Se conserva el namespace existente **geovannycode** (el prompt usa `codearti`). La base de
`k8s/notify` usa ClusterIP (80 → 8082); el overlay minikube añade NodePort 30082 como Inventory y Order.
`k8s/infra/mongodb` es solo desarrollo: MongoDB 8.0.32 fijado al mismo digest de Compose, StatefulSet,
Service headless y PVC de 1 GiB. Un ConfigMap de init crea el usuario de aplicación con `readWrite` en
`db_notify`; la aplicación nunca usa root. Los usuarios solo se crean con un volumen vacío.
Cambiar el Secret no cambia la contraseña de un usuario existente: hay que rotarla también en MongoDB.

Desde la raíz, completa las contraseñas en `.env` a partir de `.env.example` y ejecuta:

```sh
make k8s-up                              # requiere minikube instalado
make k8s-up CLUSTER=docker-desktop        # alternativa ya soportada en el repo
make k8s-validate k8s-lint CLUSTER=docker-desktop
kubectl --context docker-desktop get pods -n geovannycode
make k8s-url CLUSTER=docker-desktop
```

`make k8s-secrets` deriva los `.env` ignorados con permisos 0600. Kustomize genera Secrets con hash;
ningún valor secreto se incluye en YAML. `MONGO_DB` del ConfigMap y del init deben coincidir si se cambia
el nombre de base. Las trazas y logs OTLP están desactivados en el overlay local hasta que exista collector;
los logs de consola siguen en ECS. La plataforma crea **orders.events.v1 y orders.events.v1.dlt**, ambas con
3 particiones. En k8s están desactivadas tanto la creación de KafkaAdmin como la creación automática del consumidor.

### Réplicas, apagado y red

- `NOTIFY_CONSUMER_CONCURRENCY=1`: tres particiones admiten como máximo tres consumidores útiles.
  El HPA por CPU va de 1 a 3; la CPU es una señal imperfecta porque un consumidor puede esperar MongoDB o
  un webhook con CPU baja y lag alto. El lag del grupo `service-notify` sería la señal adecuada con KEDA;
  no se instala KEDA en esta fase ni se ponen dos autoscalers a gobernar el mismo Deployment.
- Procesamiento máximo 10 s, un registro por poll y `immediate-stop=true`. Kafka dispone de 40 s para
  terminar y confirmar offsets, Spring de 45 s por fase y el pod de 120 s incluyendo preStop de 5 s.
  Una interrupción o error de commit puede causar reentrega: el índice único `eventId` mantiene la idempotencia.
- Liveness solo comprueba el estado de la aplicación. Readiness incluye Mongo e índices. Una caída de Mongo
  retira el pod de los endpoints, sin reiniciarlo; el listener conserva los eventos mediante reintentos.
- NetworkPolicy de Mongo solo admite Notification. Notification acepta al futuro controlador ingress y
  permite salida a Mongo, Kafka, DNS y collector. Hace falta CNI compatible (por ejemplo Calico en minikube);
  Docker Desktop acepta las políticas pero esta validación no demuestra su enforcement.
- El canal local es `log`. Para webhook configura sus secretos y añade una regla de egress para el CIDR y
  puerto reales del destino. NetworkPolicy no filtra por FQDN; el control fino se hará con Istio Egress.
  No se crean Ingress, Istio ni recursos AWS.

### API y demos verificables

Postman: environment `postman/notify-k8s.postman_environment.json`, carpeta **Notify Service / k8s**.
`domainNSk8s=localhost:30082` en Docker Desktop, sin `http://` porque los requests ya lo incluyen.
En minikube usa el host y puerto que devuelve `minikube service service-notify -n geovannycode --url`.
Con NetworkPolicy aplicada por el CNI, usa port-forward hasta que exista ingress:

```sh
# Una terminal por servicio; también evita depender de NodePort en macOS.
kubectl --context minikube -n geovannycode port-forward service/service-inventory 30080:80
kubectl --context minikube -n geovannycode port-forward service/service-order 30081:80
kubectl --context minikube -n geovannycode port-forward service/service-notify 30082:80
```

Desde la raíz (Python 3, kubectl y APIs accesibles):

```sh
CLUSTER=docker-desktop python3 scripts/demo-notify-k8s.py flow
CLUSTER=docker-desktop python3 scripts/demo-notify-k8s.py outage
CLUSTER=docker-desktop python3 scripts/demo-notify-k8s.py rebalance
CLUSTER=docker-desktop python3 scripts/demo-notify-k8s.py mongo
```

Cambia `CLUSTER=minikube` según el entorno. `INVENTORY_URL`, `ORDER_URL` y `NOTIFY_URL` permiten reemplazar
las URLs completas, incluidos los prefijos `/services-*`. Cada demo crea un producto único y verifica las
notificaciones de sus órdenes. Retira temporalmente solo el HPA de Notification para evitar que revierta
el escalado manual, y restaura HPA y réplicas en `finally` incluso ante errores.

- `flow`: crea producto, crea y confirma orden, espera exactamente una notificación `sent`.
- `outage`: escala Notification a 0, confirma 10 órdenes, vuelve a 1; verifica las 10 sin duplicados.
- `rebalance`: confirma 30 órdenes mientras pasa de 1 a 3 pods; muestra miembros y particiones y comprueba
  una notificación por orden. No se altera el número de particiones.
- `mongo`: mantiene Mongo a 0 hasta observar NotReady (una recreación rápida puede durar menos que las
  probes), confirma 10 órdenes, restaura Mongo y verifica Ready, mismos UID/restarts y entrega sin pérdidas.
  Para la recreación del pod con PVC persistente: `kubectl --context docker-desktop -n geovannycode delete pod mongodb-0`.
  Observa `kubectl ... get pods -w`: un corte breve puede no alcanzar el umbral de readiness; nunca debe
  hacer fallar liveness de Notification. Los datos del PVC no se borran en ninguna demo.

Referencias: [NetworkPolicy y sus requisitos de CNI](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
y [creación inicial de usuarios de la imagen oficial MongoDB](https://hub.docker.com/_/mongo).

Validación de esta fase: ejecutada en Kubernetes de Docker Desktop (minikube no está instalado en este equipo).
`verify` pasó con 92 pruebas; kubeconform validó 40 recursos y kube-linter no encontró hallazgos.
Pasaron las demos de 1/10/30/10 órdenes y Postman (9 requests, 23 aserciones).
La caída sostenida de Mongo produjo NotReady → Ready sin reinicios; también se recreó `mongodb-0` conservando el PVC.
Evidencia local de la ejecución: `target/phase10/acceptance.md` y logs asociados (generados, no versionados).
