# Inventory Service

Microservicio reactivo con Java 25, Spring Boot 4.1.1, WebFlux y R2DBC. Flyway utiliza JDBC únicamente para versionar el esquema durante el arranque.

## Requisitos

- JDK 25 con `JAVA_HOME` configurado (`java -version`).
- Docker Desktop en ejecución y puerto 3306 disponible.
- Maven 3.9.16 mediante el wrapper.

## Arranque local

Desde la raíz del curso (directorio padre de `inventory-service`):

```sh
cp .env.example .env
# Completa DB_PASSWORD y MYSQL_ROOT_PASSWORD con valores distintos en .env.
docker compose up -d
```

Las variables `DB_USER` y `DB_PASSWORD` se asignan a `MYSQL_USER` y `MYSQL_PASSWORD` en Compose. La app usa ese usuario, nunca root. El archivo `.env` está ignorado por Git; usa comillas si los valores contienen caracteres especiales.

Exporta las variables para la app: Spring Boot no carga `.env` automáticamente.

```sh
set -a
. ./.env
set +a
cd inventory-service
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

En el primer arranque Flyway aplica V1 y V2 y crea cinco productos, incluido `AC-1550` (Lentes). Los siguientes arranques validan las migraciones sin repetir el seed. No edites migraciones ya aplicadas; añade V3, V4, etc.

```sh
curl http://localhost:8080/services-inventory/actuator/health
curl http://localhost:8080/services-inventory/actuator/health/liveness
curl http://localhost:8080/services-inventory/actuator/health/readiness
./mvnw -q verify
./mvnw dependency:tree
```

Los endpoints deben responder HTTP 200 y `{"status":"UP"}`. El health general comprueba R2DBC. Boot 4.1.1 gestiona r2dbc-mysql 1.4.3 y JUnit Jupiter 6.0.3 (ajuste respecto al JUnit 5 del contexto). Se sobrescriben las versiones gestionadas de Connector/J (26.7.0) y del BOM de Jackson (3.1.7) para incorporar correcciones de seguridad. Compose y Testcontainers fijan MySQL 8.4.11 por digest SHA-256, correspondiente a la imagen de Docker Hub publicada el 29 de septiembre de 2026. El tag mysql:8.4.12 no está disponible en Docker Hub; la documentación de Oracle anuncia ese parche para su imagen, pero no se ha podido verificar su equivalencia con esta imagen. No se afirma ausencia total de vulnerabilidades de la imagen. El test de contexto y los tests de persistencia utilizan MySQL 8.4 real mediante Testcontainers y ejecutan las migraciones Flyway. Docker debe estar activo; no requieren Compose ni credenciales locales. `@ServiceConnection` proporciona las conexiones JDBC y R2DBC del contenedor temporal.

Desde la raíz del curso, inspecciona MySQL (solicita la contraseña de `DB_PASSWORD`):

```sh
docker compose ps
docker compose exec mysql mysql -u inventory -p inventory
```

```sql
SELECT version, description, success FROM flyway_schema_history;
SELECT * FROM products;
INSERT INTO products (code, name_product, price, stock)
VALUES ('INVALID', 'Inválido', 1.00, -1); -- Debe fallar por ck_products_stock.
```

`docker compose down` conserva los datos en `mysql-data`. Las variables de inicialización de MySQL solo tienen efecto cuando el volumen está vacío; cambiar `.env` no cambia usuarios existentes.

## Tests de persistencia

Desde `inventory-service`, con JDK 25 y Docker activos:

```sh
./mvnw -q verify
```

Surefire ejecuta `*Test` y Failsafe ejecuta `*IT` durante `verify`. Los informes están en `target/surefire-reports` y `target/failsafe-reports`. Se comprueban el producto semilla, códigos duplicados, descuentos suficientes e insuficientes, códigos inexistentes y veinte descuentos concurrentes sobre diez unidades. Los contenedores usan puertos dinámicos y no modifican el MySQL local.

## Contrato y DTOs (fase 4)

El contrato fuente está en `../contracts/services-inventory.yaml` (OpenAPI 3.1.0, versión 1.1.0). Maven genera únicamente `InventoriesApi` con OpenAPI Generator 7.25.0 y utiliza los records manuales validados. La decisión API-first y las limitaciones de compatibilidad están en `../docs/adr/0001-api-contract-strategy.md`.

Desde la raíz del curso:

```sh
npx @redocly/cli lint contracts/services-inventory.yaml
cd inventory-service
./mvnw -q verify
```

Importa `../postman/services-inventory.postman_collection.json`. Los endpoints de negocio están implementados; ejecuta la carpeta local de Postman en orden. La colección original no estaba disponible y sus cambios de carpetas Order/Notify quedan pendientes. Si contenía claves reales, deben rotarse además de sustituirlas por `{{apiKey}}`.

## Endpoints y streaming (fase 5)

El perfil local habilita CORS únicamente para orígenes HTTP de localhost y 127.0.0.1. Swagger UI está en <http://localhost:8080/services-inventory/swagger-ui.html> y OpenAPI en <http://localhost:8080/services-inventory/v3/api-docs>.

```sh
curl -i -H 'Content-Type: application/json' -d '{"idProduct":"PRD-1001","nameProduct":"Lentes","price":123.5,"stock":50}' http://localhost:8080/services-inventory/inventories
curl -i -X PUT -H 'Content-Type: application/json' -d '{"orderCount":5}' http://localhost:8080/services-inventory/inventories/PRD-1001
curl -N -H 'Accept: application/x-ndjson' 'http://localhost:8080/services-inventory/inventories?delayMs=500'
curl -N -H 'Accept: text/event-stream' 'http://localhost:8080/services-inventory/inventories?delayMs=500'
```

El POST devuelve 201 y Location; repetir el mismo código devuelve 409. Stock cero al crear o cantidades fuera de 1..5 devuelven 400 con errors. Un código inexistente devuelve 404; stock insuficiente devuelve 409. Los descuentos y su lectura posterior comparten una transacción R2DBC. No reintentes automáticamente un PUT cuyo resultado sea incierto.

Desde la raíz del curso, sirve los viewers:

```sh
python3 -m http.server 5500 --bind 127.0.0.1 --directory web
```

Abre <http://127.0.0.1:5500/stream-viewer.html> o <http://127.0.0.1:5500/stream-event-viewer.html> y pulsa Conectar. El primero lee NDJSON incrementalmente y el segundo escucha eventos SSE inventory, cuyo id es el código del producto. delayMs admite 0..2000; por defecto es 0.

Para ejecutar la colección disponible sin la interfaz de Postman, desde la raíz:

```sh
npx --yes newman run postman/services-inventory.postman_collection.json --folder local
```

Las pruebas HTTP de `InventoryApiIT` usan su propio MySQL en Testcontainers y no modifican la base local. Comprueban también la lectura consistente durante descuentos concurrentes, la entrega incremental de NDJSON/SSE, CORS y las rutas de documentación.

## Estrategia de pruebas (fase 6)

| Nivel | Clase | Qué cubre |
|---|---|---|
| Unitario | `InventoryServiceTest`, `InventoryMapperTest`, `GlobalExceptionHandlerTest`, `InventoryDtoValidationTest` | Lógica del servicio con Mockito y StepVerifier, mapeo, ProblemDetail saneado y límites de los DTOs |
| Slice web | `InventoryControllerTest` | `@WebFluxTest` + `@MockitoBean`: status, Location, Content-Type, ProblemDetail, NDJSON y SSE |
| Persistencia | `ProductRepositoryIT` | `@DataR2dbcTest` con MySQL real y Flyway |
| End-to-end | `InventoryApiIT` | Flujo crear → consultar → descontar → agotar → 409 bajo `/services-inventory` |
| Concurrencia | `StockConcurrencyIT` | 50 PUT concurrentes sobre stock 10: exactamente 10 × 200, 40 × 409 y stock final 0 |
| Arquitectura | `ArchitectureTest` | `api` no depende de `infrastructure`, `domain` no depende de Spring y no hay ciclos entre paquetes |

```sh
./mvnw verify
open target/site/jacoco/index.html
```

JaCoCo instrumenta Surefire y Failsafe; en `verify` genera el reporte y exige ≥ 80 % de líneas y ≥ 70 % de ramas (se excluyen la clase de arranque y los records sin lógica). Cada test usa códigos UUID propios y no depende del orden ni de datos de otro test; el seed solo se lee.

Para repetir la prueba de concurrencia de forma aislada:

```sh
for i in 1 2 3 4 5; do ./mvnw -q verify -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=StockConcurrencyIT -Djacoco.skip=true || break; done
```

ArchUnit se configura con `resolveMissingDependenciesFromClassPath=false` (`src/test/resources/archunit.properties`): con un JDK más nuevo que el bytecode que soporta ArchUnit, evita intentar leer las clases del JRE, que no intervienen en las reglas.

## Observabilidad (fase 7)

Todo se sirve en el puerto 8080 bajo `/services-inventory/actuator`. Solo se exponen `health`, `info`, `prometheus` y `metrics`.

| Endpoint | Uso | Contenido |
|---|---|---|
| `/actuator/health/liveness` | livenessProbe | Solo `livenessState`. No consulta MySQL: reiniciar el pod no arregla una caída de la base. |
| `/actuator/health/readiness` | readinessProbe | `readinessState` + `r2dbc`. Con MySQL caído devuelve 503 `DOWN` y Kubernetes deja de enrutar tráfico al pod. |
| `/actuator/info` | Versión desplegada | `build` (goal `build-info`) y `git` (git-commit-id-maven-plugin; vacío si el directorio no es un repositorio Git). |
| `/actuator/prometheus` | Scrape de Prometheus | Métricas HTTP, JVM, pool R2DBC y de negocio, todas con la etiqueta `application="service-inventory"`. |

`show-details: when-authorized` no muestra componentes porque el servicio no tiene seguridad; el detalle aparecerá cuando se añada autenticación.

### Métricas

- `http_server_requests_seconds_bucket`: histograma con buckets SLO de 100 ms, 300 ms y 1 s. Los percentiles se calculan en Prometheus, porque los percentiles calculados en el cliente no se pueden agregar entre pods y el registro de Prometheus no los publica junto al histograma:

  ```promql
  histogram_quantile(0.95, sum by (le, uri) (rate(http_server_requests_seconds_bucket{application="service-inventory"}[5m])))
  ```

- `inventory_stock_decrease_total{result="ok|insufficient|not_found"}` e `inventory_product_registered_total`: contadores de negocio registrados con `MeterRegistry` en `InventoryService`. Las tres series de `result` existen desde el arranque (valor 0).

No se usa `@Observed`: `ObservedAspect` (Micrometer 1.17) solo entiende `CompletionStage`. Sobre un método que devuelve `Mono` cerraría la observación al ensamblar el pipeline, no al terminar, y daría spans y timers falsos de 0 ms. El detalle por operación ya lo aportan el span HTTP y los spans de R2DBC.

### Trazas

`spring-boot-starter-opentelemetry` exporta por OTLP/HTTP a `${OTEL_EXPORTER_OTLP_ENDPOINT:http://localhost:4318}/v1/traces`, con un muestreo de `${TRACING_SAMPLING:1.0}`. Boot 4.1 también interpreta las variables estándar `OTEL_*` (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_TRACES_SAMPLER_ARG`, `OTEL_TRACES_EXPORTER=none`, etc.) y les da prioridad sobre `application.yml`.

- **Propagación:** se acepta `traceparent` (W3C) y B3. Una llamada de Order con `traceparent` continúa su traza: el span HTTP de Inventory queda como hijo del span de Order.
- **Spans de BD:** se añade `io.r2dbc:r2dbc-proxy` (runtime). Con él, `R2dbcObservationAutoConfiguration` de Boot envuelve el `ConnectionFactory` y emite un span `query` por sentencia, con el SQL parametrizado. Los valores de los parámetros no se registran (`management.observations.r2dbc.include-parameter-values=false`, valor por defecto). Es la vía oficial de Boot; la alternativa, el agente Java de OpenTelemetry, duplicaría la instrumentación de Micrometer.
- **Contexto reactivo:** `spring.reactor.context-propagation=auto` restaura `traceId`/`spanId` en el MDC en cada salto de hilo de Reactor, de modo que los logs de `InventoryService` llevan la traza de la petición.
- **Métricas por OTLP:** desactivado (`management.otlp.metrics.export.enabled=false`). El modelo del curso es el scrape de Prometheus. Para ver métricas en el Grafana local sin configurar un scrape, arranca con `OTEL_METRICS_EXPORTER=otlp`.

### Logs

- **Perfil `local`:** formato legible. Boot añade `[traceId-spanId]` a cada línea, o un hueco en blanco cuando no hay petición en curso.
- **Perfiles `docker` y `k8s`:** JSON ECS 8.11 en stdout, con `service.name`, `service.version`, `service.environment`, `trace.id` y `span.id`. Promtail/Alloy (Loki) o Filebeat (ELK) los recogen de stdout y correlacionan con Tempo/Jaeger por `trace.id`.
- **Datos registrados:** se registran códigos de producto y cantidades, nunca credenciales ni valores de parámetros SQL. Los errores 500 no exponen el mensaje interno al cliente.

### Stack local (Grafana + Loki + Tempo + Prometheus)

Desde la raíz del curso:

```sh
docker compose --profile observability up -d
set -a; . ./.env; set +a
cd inventory-service
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

```sh
curl -X PUT -H 'Content-Type: application/json' \
  -H 'traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' \
  -d '{"orderCount":1}' http://localhost:8080/services-inventory/inventories/AC-1550
```

Abre Grafana en <http://localhost:3000> (Explore → Tempo) y busca el trace ID `4bf92f3577b34da6a3ce929d0e0e4736`. Verás el span `http put /inventories/{productId}` con los spans `query` del `UPDATE` y del `SELECT`. Este curl descuenta una unidad real de AC-1550 en tu base local.

### Tests

- `ObservabilityIT`:
  - comprueba que `/actuator/prometheus` contiene `http_server_requests_seconds` con los buckets SLO, `inventory_stock_decrease_total` por resultado e `inventory_product_registered_total`;
  - comprueba que un `traceparent` entrante se continúa y que la traza incluye spans de R2DBC, usando un exportador en memoria, sin colector;
  - comprueba que `info` expone `build` y que `env`, `beans` y `configprops` responden 404.
- `HealthProbesIT`: detiene el MySQL de Testcontainers y comprueba que readiness pasa a 503 `DOWN` mientras liveness sigue en 200 `UP`.

`spring-boot-starter-opentelemetry-test` desactiva la exportación de métricas y trazas en el resto de los `@SpringBootTest`, que así no intentan conectar con `localhost:4318`.
