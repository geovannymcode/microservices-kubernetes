# service-notify

Microservicio reactivo de notificaciones. Consume los eventos de órdenes que publica Order en Kafka (`orders.events.v1`), registra una notificación por evento en MongoDB, la envía por un canal y expone una API de solo lectura.

Estado: **fase 3**. Persistencia reactiva en MongoDB y API generada desde el contrato. Las operaciones aún responden 501, y todavía no hay listener ni servicio.

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
| `KAFKA_BOOTSTRAP_SERVERS` | sin uso todavía (entra con el listener) |

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

**`NotifyApiDelegateImpl`** sobrescribe las dos operaciones. Por ahora responden 501 con un TODO, y la lógica llega en la fase 5. `NotifyApiTest` comprueba por reflexión que ninguna operación queda con el 501 por defecto de la interfaz generada (`skipDefaultInterface` debe seguir en `false` con `delegatePattern`).

**Validación.** `limit` admite de 1 a 500, `orderId` debe ser 1 o mayor y `notifyId` debe tener 24 caracteres hexadecimales. Estas restricciones vienen del contrato y las aplica el controlador generado (`@Validated`). Esa validación lanza `ConstraintViolationException`, que sin manejador sería un 500. Por eso `GlobalExceptionHandler` la convierte en un 400 `ProblemDetail` (`validation-error`, con `errors[]` y mensajes en español), con el mismo formato que Inventory y Order. Un `status` desconocido da 400 `invalid-request`.

**`NotifyMapper`** (MapStruct 1.6.3, `componentModel = "spring"`) convierte `NotificationDocument` en `NotifyResponse`:

- `Instant` → `OffsetDateTime` en UTC;
- `eventId` → `UUID`;
- `eventType`, `channel` y `status` pasan por su valor en el contrato (`fromValue`), nunca por el nombre de la constante. Un valor desconocido falla en lugar de mapearse a `null`.

```sh
curl -i http://localhost:8082/services-notify/notify                 # 501 (fase 5)
curl -i "http://localhost:8082/services-notify/notify?limit=9999"    # 400 validation-error
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
  - `NotifyApiTest`, slice web: el delegate cubre todas las operaciones, responde 501 y la validación generada da 400 con `ProblemDetail`.
- **Integración** (Failsafe, `*IT`): `NotificationRepositoryIT`. Corre contra `mongo:8.0.32` en Testcontainers 2 (`@ServiceConnection`, con el mismo digest que Compose) y cubre:
  - el id, `createdAt` y `version` asignados al guardar;
  - `findByEventId`;
  - el `eventId` duplicado (`DuplicateKeyException`);
  - los filtros con su orden y su límite;
  - el `status` en minúscula, leído del documento crudo;
  - el bloqueo optimista;
  - la idempotencia de los índices.

`./mvnw verify` necesita Docker.
