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

El contrato fuente está en `../contracts/services-inventory.yaml` (OpenAPI 3.1.0, versión 2.0.0: códigos de producto solo en mayúsculas). Maven genera únicamente `InventoriesApi` con OpenAPI Generator 7.25.0 y utiliza los records manuales validados. Enfoque API-first: el contrato es la fuente de verdad, la interfaz se genera en cada build y nunca se edita, y los DTOs son records validados que se conectan mediante `importMappings`. Los errores se modelan como `ProblemDetail`.

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

El POST devuelve 201 y Location; repetir el mismo código devuelve 409. Stock cero al crear o cantidades fuera de 1..5 devuelven 400 con errors. Un código inexistente devuelve 404; stock insuficiente devuelve 409. Los descuentos y su lectura posterior comparten una transacción R2DBC.

Desde la revisión de código final:

- **Reintentos del PUT:** solo son seguros con el header `Idempotency-Key`.
  - La clave se registra en la misma transacción que el `UPDATE` (tabla `stock_movements`, V3).
  - Un reintento con la misma clave y el mismo cuerpo responde 200 con el estado actual, sin volver a descontar.
  - La misma clave con otro producto u otra cantidad responde 422 `idempotency-key-reused`.
  - Un 404 o 409 no consume la clave.
  - Formato (contrato v2.1.0): 1..100 caracteres de `[A-Za-z0-9._:-]`, por ejemplo `order:12345`. La migración V5 ensancha la columna; V3 no se edita.
  - Se mantiene 422 (no 409) para una clave reutilizada, como en el borrador IETF del header `Idempotency-Key`: el 409 de Inventory significa stock insuficiente y Order lo traduce en cancelar la orden, así que un conflicto de clave no debe confundirse con él.
  - Sin header, el PUT se comporta como antes. Order debe enviar el id de la orden como clave.
- **Tipos estrictos en el JSON:** `{"orderCount": 1.9}` o `{"orderCount": "3"}` devuelven 400 (`spring.jackson.deserialization.accept-float-as-int` y `spring.jackson.mapper.allow-coercion-of-scalars` están a `false`). Antes, Jackson los convertía y descontaba stock.
- **Fallos de base de datos:** MySQL caído o el pool agotado devuelven 503 `database-unavailable` con `Retry-After: 5` y un WARN de una línea, no un 500 con stacktrace.
- **Paginación:** `GET /inventories?page=0&size=20` en JSON, NDJSON y SSE. `size` admite de 1 a 100 (20 por defecto); para recorrer todo el catálogo, pide páginas hasta recibir menos de `size` elementos. Con `delayMs`, las filas de la página se leen antes de aplicar la demora, así que un cliente lento no ocupa una conexión del pool.
- **Métricas después del commit:** el descuento usa `TransactionalOperator` y los contadores `inventory.stock.decrease` se actualizan tras el commit o el rollback; un commit fallido no cuenta como `ok`.
- **Pool R2DBC:** `max-acquire-time: 1s`; con el reintento de adquisición, el peor caso son ~2 s, dentro del timeout de 3 s de la readiness. Lo prueba `DatabaseSaturationIT`.

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
- **OTLP:** `OpenTelemetryLogsConfiguration` conecta Logback al SDK de OpenTelemetry (`opentelemetry-logback-appender-1.0`, sin `logback-spring.xml`). Con `OTEL_LOGS_EXPORTER=otlp` (Compose y Kubernetes) los logs llegan a Loki con su `trace_id`; en local está apagado porque no hay colector.
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

## Contenedor (fase 8)

Decisiones y mediciones:

- **Imagen:** multi-stage con una JRE mínima (`jlink`) sobre `gcr.io/distroless/cc-debian13`, usuario `10001`, sin shell, sin JDK y sin fuentes. Pesa 245 MB.
- **Arranque:** el AOT cache de Java 25 baja el arranque de 2,2 s a 1,0 s.

Desde la raíz del curso:

```sh
docker compose up -d --build                         # MySQL + service-inventory, ambos healthy
docker compose --profile observability up -d --build # además Grafana/Loki/Tempo/Prometheus
docker compose ps
```

- **Puerto:** la app publica `127.0.0.1:${INVENTORY_PORT:-8080}`. Si el 8080 está ocupado (por ejemplo, por la app lanzada con `spring-boot:run`), usa `INVENTORY_PORT=8082 docker compose up -d`.
- **Variables de entorno:** Compose pasa solo `DB_NAME`, `DB_USER` y `DB_PASSWORD` desde `.env`. El perfil `docker` fija el host `mysql:3306`, porque `DB_HOST` y `DB_PORT` del `.env` describen el puerto publicado en tu máquina, no la red de Compose.
- **Trazas sin el perfil observability:** las trazas se envían a `http://otel-lgtm:4318`. Si no levantas el perfil, define `OTEL_TRACES_EXPORTER=none` para evitar avisos de exportación.
- **Health check:** se ejecuta con una clase Java incluida en la imagen (`docker/HealthCheck.java`), porque distroless no trae `curl` ni `wget`.

Imagen sin Compose:

```sh
docker build --build-context contracts=contracts -t geovannycode/service-inventory:0.0.1-SNAPSHOT inventory-service
docker image ls geovannycode/service-inventory
docker top service-inventory -eo uid,pid,args        # UID 10001; la imagen no contiene el binario id
```

Medir el efecto del AOT cache (con `-XX:AOTMode=off` la JVM lo ignora):

```sh
docker logs service-inventory 2>&1 | grep 'Started InventoryServiceApplication'
docker run --rm --entrypoint java geovannycode/service-inventory:0.0.1-SNAPSHOT \
  -XX:AOTCache=/app/app.aot -XX:AOTMode=on -Dspring.context.exit=onRefresh -Dspring.flyway.enabled=false -jar /app/app.jar
```

`-XX:AOTMode=on` hace que la JVM falle si el cache no es válido. Sirve para detectar un cache que la JVM descartaría en silencio, por ejemplo tras cambiar opciones de la JVM sin regenerarlo.

### Escaneo de vulnerabilidades

Con Trivy instalado (`brew install trivy`):

```sh
trivy image --scanners vuln --severity HIGH,CRITICAL geovannycode/service-inventory:0.0.1-SNAPSHOT
```

Sin instalarlo, mediante su imagen oficial:

```sh
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v trivy-cache:/root/.cache/ \
  aquasec/trivy:0.75.0 image --scanners vuln --severity HIGH,CRITICAL geovannycode/service-inventory:0.0.1-SNAPSHOT
```

Resultado actual: 0 HIGH y 0 CRITICAL. Se corrigió Jackson 2.21.5 → 2.21.7 (transitivo de springdoc; `jackson-2-bom.version` en el `pom.xml`). Además se cambió `distroless/java-base` por `distroless/cc`, que no incluye las librerías de fontconfig con CVE sin parche.

### CI

`.github/workflows/inventory-ci.yml`:

1. Ejecuta `./mvnw verify` con Temurin 25 y caché de Maven.
2. Construye la imagen (`linux/amd64`).
3. En `main`, la publica en `ghcr.io/<owner>/service-inventory` con las etiquetas `sha-<commit>` y `latest`.

En pull requests solo se construye la imagen, sin publicarla.


**Build en CI (BuildKit con driver `docker-container`).** El Dockerfile resuelve dos problemas que no aparecían en las builds locales:

- **`unzip` en la etapa de build.** La imagen `eclipse-temurin:25-jdk` no lo trae, y sin él `mvnw` descarga Maven en `.tar.gz` en lugar del `.zip`. El hash fijado en `maven-wrapper.properties` es el del `.zip` (el que exige `mvnw.cmd` en Windows), así que la validación fallaba. En local no pasaba porque Maven ya estaba en la caché `/root/.m2`.
- **`OTEL_TRACES_EXPORTER=none` en la etapa `aot`.** El builder de `setup-buildx-action` inyecta en cada `RUN` variables `OTEL_*` con un endpoint `unix://` para su propio tracing. Boot les da prioridad sobre cualquier propiedad y el training run fallaba al crear el exportador OTLP. Un `ENV` del Dockerfile prevalece sobre lo inyectado, y esa etapa no llega a la imagen final.

Para reproducir la build de CI en local, con una caché vacía:

```sh
docker buildx create --name ci --driver docker-container
docker buildx build --builder ci --platform linux/amd64 --build-context contracts=contracts --load inventory-service
```

## Kubernetes (fase 9)

Los manifiestos están en `../k8s` y usan Kustomize (`kubectl apply -k`):

```text
k8s/
  namespace/                      namespace geovannycode, aplicado aparte: borrar las apps no borra las BD
  infra/mysql/                    StatefulSet mysql:8.4 + Service headless + PVC + Secret + NetworkPolicy: solo dev (en EKS será RDS)
  infra/postgres/, infra/kafka/   dependencias de Order (ver README de Order)
  inventory/base/                 ServiceAccount, ConfigMap (generado), Deployment, Service, HPA, PDB, NetworkPolicy
  inventory/overlays/minikube/    Secret de BD, NodePort 30080, imagePullPolicy Never, muestreo 1.0, sin exportador OTLP
  inventory/overlays/eks/         placeholder con TODOs (ECR, RDS, Secrets Manager, IRSA, recursos, HPA min 2)
  order/                          manifiestos de Order (ver README de Order)
  overlays/minikube/, overlays/eks/  kustomization raíz que lo incluye todo (dry-run, diff, GitOps)
  addons/metrics-server/          metrics-server para Docker Desktop (minikube usa su addon)
  load/inventories.js             carga k6 para la prueba del HPA
```

### Decisiones

- **Secretos:** ninguno se versiona. `k8s/infra/mysql/.env` y `k8s/inventory/overlays/minikube/.env` están ignorados por Git y generan los Secret con `secretGenerator`. `make k8s-secrets` los crea a partir del `.env` de la raíz (las mismas credenciales que Compose); también hay un `.env.example` en cada carpeta.
- **ConfigMap:** también se genera con `configMapGenerator`. Su nombre lleva un hash del contenido, así que cambiar la configuración provoca un rollout; con un ConfigMap estático los pods seguirían con los valores viejos.
- **Réplicas:** el Deployment no declara `replicas`. Las gestiona el HPA (min 1, max 4); si `replicas` estuviera en el manifiesto, cada `kubectl apply` reiniciaría la escala a ese valor.
- **Sin límite de CPU:** con cuotas CFS, la JVM se queda sin CPU justo en el arranque (JIT, carga de clases) y los probes fallan. Se pide 250m, que es lo que usan el scheduler y el porcentaje del HPA, y la CPU sobrante del nodo se aprovecha sin throttling. La memoria sí está limitada a 768Mi. En el pod, `MaxRAMPercentage=60` (heap de ~460Mi) y `MaxDirectMemorySize=96m` dejan margen para el metaspace, el code cache y los hilos; `ActiveProcessorCount=2` evita que la JVM se dimensione para todos los cores del nodo (ver los resultados verificados).
- **Probes:**
  - `startupProbe` admite hasta 60 s (30 × 2 s) antes de que empiece a contar la liveness.
  - La liveness no consulta MySQL; la readiness sí, a través de `r2dbc`.
  - Si cae la BD, el pod sale del Service pero no se reinicia.
- **Apagado:** `preStop` con la acción nativa `sleep` de Kubernetes (1.30+), porque la imagen distroless no tiene binario `sleep`. Los 5 s dan tiempo a que los Endpoints dejen de enviar tráfico antes de que Spring empiece el apagado graceful (20 s), y el total cabe en `terminationGracePeriodSeconds: 30`.
- **Seguridad:**
  - `runAsNonRoot` con uid 10001.
  - Raíz de solo lectura, con `emptyDir` en `/tmp` para los datos de rendimiento de la JVM y los nativos de Netty.
  - `capabilities: drop ALL`, `seccompProfile: RuntimeDefault` y sin token de ServiceAccount.
- **Labels:**
  - `app.kubernetes.io/{name,part-of,version,component}`.
  - En el pod, además, `app` y `version`, que son las que usan Istio y Kiali.
  - El selector usa solo `app.kubernetes.io/name`, porque es inmutable.
- **Service:** ClusterIP con el puerto 80 nombrado `http` (Istio deduce el protocolo del nombre). Order lo llamará en `http://service-inventory.geovannycode.svc.cluster.local/services-inventory`. El overlay local lo cambia a NodePort.
- **PDB:**
  - En base, `maxUnavailable: 1`, que con una sola réplica no bloquea un drain.
  - El overlay `eks` lo endurece a `minAvailable: 1` y sube el HPA a min 2.
  - `unhealthyPodEvictionPolicy: AlwaysAllow` evita que un pod en CrashLoop bloquee un drain.

### Despliegue

Desde la raíz del curso:

```sh
make k8s-up                          # minikube: start, metrics-server, imágenes en su Docker, infra, migración de Order, apps y URLs
make k8s-up CLUSTER=docker-desktop   # Kubernetes de Docker Desktop (modo kubeadm: comparte las imágenes de Docker)
make k8s-validate                    # kubectl apply -k --dry-run=server
make k8s-lint                        # kubeconform + kube-linter
kubectl get pods -n geovannycode
```

- En **minikube**, `make k8s-url` ejecuta `minikube service service-inventory -n geovannycode --url`. Con el driver Docker en macOS abre un túnel en `127.0.0.1:<puerto>` y se queda bloqueado: mantenlo abierto mientras usas Postman.
- En **Docker Desktop**, el NodePort fijo se publica en `http://localhost:30080`.
- `make k8s-down` borra solo las apps; MySQL, PostgreSQL, Kafka y sus PVC se conservan.
- `make k8s-purge` borra el namespace entero, incluidos los datos.
- `make k8s-up` reconstruye la imagen y hace `rollout restart`: el tag es fijo y `imagePullPolicy: Never`, así que sin el reinicio los pods seguirían con la imagen anterior.

Validación estática sin clúster (requiere los `.env` de `make k8s-secrets`, porque los lee el `secretGenerator`):

```sh
kubectl kustomize k8s/overlays/minikube | docker run --rm -i ghcr.io/yannh/kubeconform:v0.8.0@sha256:faffaf43f95aa6425306e1ab8d6fcad72acb9049158f38e574c085ea1ec0f64e -strict -summary -
kubectl kustomize k8s/overlays/minikube | docker run --rm -i stackrox/kube-linter:v0.8.3@sha256:f2bfce7879206d32f69ab6572c376f916643f54ca291ac38cf7d01ef591ff3f9 lint -
```

### Postman

La carpeta **Inventory Service / k8s** repite las 16 requests de `local` contra `{{domainISk8s}}`:

1. Actuator: health, liveness, readiness y prometheus.
2. Listado y consulta por id (incluido un id inexistente).
3. Alta, alta duplicada y alta inválida.
4. Update y update con error.
5. Preparar un producto sin stock y descontar de él (409).
6. NDJSON y SSE.

Importa también `postman/inventory-minikube.postman_environment.json` y pon en `domainISk8s` la URL de `make k8s-url`. Desde la terminal:

```sh
npx --yes newman@6 run postman/services-inventory.postman_collection.json --folder k8s \
  -e postman/inventory-minikube.postman_environment.json --env-var domainISk8s=http://127.0.0.1:<puerto>
```

### Caída de MySQL

**Trade-off de acoplar la readiness a la BD.** Si MySQL cae, *todas* las réplicas quedan NotReady a la vez:

- Los clientes reciben "no endpoints" o "connection refused" del Service, en lugar del 503 `database-unavailable`, que solo verían peticiones que lleguen al pod (por ejemplo, durante los ~6 s antes de que falle la readiness).
- Un rollout con `maxUnavailable: 0` queda bloqueado hasta que vuelva la BD, porque los pods nuevos no pasan a Ready.

Se acepta porque el servicio no puede atender ninguna operación sin MySQL, y sacar los pods del balanceo es la señal más rápida para el circuit breaker de Order. Si algún día hubiera lecturas cacheadas que pudieran servirse sin BD, habría que quitar `r2dbc` del grupo de readiness y confiar solo en el 503.


```sh
kubectl scale statefulset mysql -n geovannycode --replicas=0
kubectl get pods -n geovannycode -w          # service-inventory pasa a 0/1, RESTARTS sigue en 0
kubectl get endpoints service-inventory -n geovannycode   # la IP aparece en notReadyAddresses
kubectl scale statefulset mysql -n geovannycode --replicas=1
```

Borrar el pod (`kubectl delete pod mysql-0`) suele no bastar para verlo, porque el StatefulSet lo recrea en unos 4 s, antes de que la readiness falle dos veces (2 × 5 s). Mientras el pod está NotReady, las peticiones a través del Service fallan (no hay endpoints listos); esa es justamente la señal para que Order abra su circuit breaker.

### Prueba del HPA

Requiere metrics-server: `minikube addons enable metrics-server` o, en Docker Desktop, `kubectl apply -k k8s/addons/metrics-server` (lo hace `make k8s-up`).

```sh
kubectl get hpa -n geovannycode -w        # en una terminal
make k8s-load                         # en otra: k6 dentro del clúster, 50 usuarios virtuales durante 3 min contra GET /inventories
```

La carga se lanza dentro del clúster contra el Service ClusterIP, como lo hará Order. Así no depende del túnel de `minikube service`, que limitaría el throughput. Con `hey` desde tu máquina el efecto es el mismo:

```sh
hey -z 3m -c 50 <url>/services-inventory/inventories
```

### Resultados verificados (Kubernetes de Docker Desktop v1.32.2, 2026-10-03)

| Criterio | Resultado |
|---|---|
| `kubectl apply -k --dry-run=server` (mysql + overlay) | Sin errores; `kubectl diff -k` vacío tras aplicar (idempotente) |
| kubeconform `-strict` / kube-linter 0.8.3 | 19/19 recursos válidos / sin hallazgos |
| Rollout | `service-inventory` Running 1/1 y `mysql-0` Running 1/1, 0 reinicios |
| Postman `k8s` (newman) | 16 requests, 21 aserciones, 0 fallos |
| MySQL a 0 réplicas | Inventory NotReady a los ~6 s, fuera de los endpoints y con 0 reinicios; Ready 3 s después de que MySQL vuelve |
| HPA con k6 (50 VUs, 3 min) | 2,1 M requests a ~11.800 req/s, 0 % de errores, p95 7,5 ms; escala de 1 a 4 réplicas y vuelve a 1 tras la ventana de 120 s; 0 reinicios |
| Arranque en el pod | 1,1–1,2 s con AOT cache: el 96 % de las clases sale del cache, también con SerialGC y los flags de k8s |

**OOMKill encontrado y corregido.** En la primera prueba de carga, el pod se reinició por `OOMKilled` con el límite de 768Mi. Lo reproduje con una sola réplica: la memoria subía unos 120Mi cada 15 s. Hay dos causas:

1. El techo de memoria directa de Netty es, por defecto, igual al heap máximo. Con `MaxRAMPercentage=75`, heap y memoria directa podían sumar ~1,1Gi.
2. Sin límite de CPU, la JVM ve los 12 cores del nodo y dimensiona para ellos los hilos de GC y JIT y los event loops y arenas de Netty.

El Deployment sobrescribe `JAVA_TOOL_OPTIONS` con `-XX:MaxRAMPercentage=60 -XX:MaxDirectMemorySize=96m -XX:ActiveProcessorCount=2 -XX:+ExitOnOutOfMemoryError`. Con estos flags, la memoria se estabiliza en 639Mi tras 2,5 min de carga sostenida sobre una sola réplica a 4,7 cores.

`ActiveProcessorCount` es imprescindible cuando no se pone límite de CPU. En un nodo de EKS con 32 o 96 cores, la JVM crearía cientos de hilos y arenas para un pod que pide 250m.

El Deployment no tiene límite de CPU, así que un solo pod llegó a 5,1 cores bajo carga. El HPA mide el uso respecto al request (250m), no respecto a lo que el pod consume realmente. En EKS, ese request debe reflejar el consumo sostenido real para que el escalado tenga sentido (es uno de los TODO del overlay).
