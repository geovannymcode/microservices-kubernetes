# service-notify

Microservicio reactivo de notificaciones. Consume los eventos de órdenes que publica Order en Kafka (`orders.events.v1`), registra una notificación por evento en MongoDB, la envía por un canal y expone una API de solo lectura.

Estado: **fase 5**. Consume `orders.events.v1`, guarda una notificación por evento en MongoDB, la envía por el canal `log` y expone la API de solo lectura del contrato v2.0.0.

## Requisitos

- JDK 25 y Maven 3.9 o superior (se usa el wrapper `./mvnw`, Maven 3.9.16).
- MongoDB 8.0 con un usuario de la aplicación en la base `db_notify`.

## Stack

Java 25, Spring Boot 4.1.1, WebFlux, Spring Data MongoDB reactivo (`spring-boot-starter-data-mongodb-reactive`), Actuator y Bean Validation.

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
| `NOTIFICATION_SENDER_TYPE` | `log` (el canal `webhook` llega en la fase 6) |

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

- **Sin duplicados.** Lo garantiza el índice único de `eventId`: el servicio inserta y, si recibe `DuplicateKeyException`, lee la notificación existente. No hace "consultar y luego insertar". Antes de procesar espera a que exista el índice (`MongoIndexInitializer.ready()`).
- **Reintentos.** `DefaultErrorHandler` con backoff exponencial: 3 entregas en total, con esperas de 1 s y 2 s y un tope de 4 s (`notify.retry.*`). En la última, el listener marca la notificación FAILED y el `DeadLetterPublishingRecoverer` publica el evento en `orders.events.v1.dlt`, en la misma partición y con los headers `kafka_dlt-exception-*`.
- **Topic DLT.** Lo crea un `NewTopic` con 3 particiones, solo en los perfiles `local`, `docker` y `test`.
- **Mensajes:**
  - completada: `La orden {orderId} ({codeProduct} x{quantity}) fue completada.`
  - cancelada: `... fue cancelada: {motivo}.`, donde el motivo es `el producto no existe` o `no hay stock suficiente`.
- **Canal.** `LogNotificationSender` (`notification.sender.type=log`, el valor por defecto) escribe la notificación en el log, en INFO. El puerto `NotificationSender` recibe un `NotificationMessage` del dominio, no el documento de Mongo, para no crear un ciclo entre el dominio y la persistencia.

Para ver el consumidor con el sistema completo (Kafka, Order e Inventory de Compose):

```sh
docker exec order-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group service-notify
docker exec -it notify-mongo mongosh -u notify -p --authenticationDatabase db_notify db_notify \
  --eval 'db.notify_orders.find({}, {_id:0, orderId:1, eventType:1, status:1, attempts:1, message:1}).sort({createdAt:-1}).limit(5)'
```

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

Actuator expone solo `health` e `info`; `/services-notify/actuator/env` responde 404.

## Verificación

```sh
./mvnw -q verify
```

- **Unitarios** (Surefire, `*Test`):
  - `contextLoads`;
  - `NotifyMapperTest` (todos los campos, fechas en UTC, enums por su valor y rechazo de valores desconocidos);
  - `NotifyApiTest`, slice web con el servicio simulado (`@MockitoBean`), que comprueba:
    - status, `Content-Type` y cuerpo de JSON, NDJSON y SSE;
    - el paso de filtros y límite;
    - los `ProblemDetail` 400, 404 y 500;
  - `NotificationServiceTest` (Mockito + StepVerifier): una prueba por fila de la tabla de decisión, más los mensajes, `markFailed`, la espera al índice y las consultas (incluido el 404).
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
  - canal que falla una vez → SENT con `attempts = 2`.

`./mvnw verify` necesita Docker.
