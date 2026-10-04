# CLAUDE.md — service-order

## Proyecto
Microservicio **Order Service** del curso "Arquitectura de Microservicios con Spring Boot y Kubernetes". Registra órdenes y, al confirmarlas, descuenta stock en **Inventory Service** por HTTP síncrono protegido con Resilience4j. Publica eventos en Kafka para Notification Service.

**Order se construye desde cero** a partir de un esqueleto de Spring Initializr, igual que Inventory. No hay código previo que migrar.

Inventory ya está implementado en este mismo repo. Úsalo como referencia de estilo: mismas convenciones, mismo formato de errores, misma estructura de tests, compose y manifiestos.

**Nombres:** los prompts dicen `service-order` y `service-inventory`. Si en el repo las carpetas, el paquete raíz o los nombres de servicio son otros (por ejemplo `order-service` / `inventory-service`), usa los que ya existen y mantenlos coherentes con lo hecho en Inventory. No renombres nada.

## Stack obligatorio
- Java **25 LTS**, Maven Wrapper 3.9.x
- Spring Boot **4.1.x** (Spring Framework 7, Jackson 3 `tools.jackson.*`, starters modulares)
- **Reactivo**: WebFlux + Spring Data R2DBC + `org.postgresql:r2dbc-postgresql`
- PostgreSQL **17**; migraciones con **Liquibase** vía JDBC (changelog YAML, cada changeSet con `rollback`)
- **API-first**: `openapi-generator-maven-plugin` genera interfaces y DTOs del servidor desde `contracts/services-order.yaml`
- Resiliencia: `resilience4j-spring-boot4` + `resilience4j-reactor`. **Sin Spring Cloud.**
- MapStruct para mapear. **Sin Lombok.** Entidades como `record`.
- Tests: JUnit 5, `StepVerifier`, `WebTestClient`, Testcontainers 2.x con `@ServiceConnection`, WireMock para simular Inventory

## Convenciones
- Paquete raíz: el del esqueleto (por ejemplo `com.geovannycode.order`):
  ```
  order/
    api/            -> OrderApiDelegateImpl, GlobalExceptionHandler, OrderMapper
    application/    -> OrderService (casos de uso)
    domain/         -> OrderStatus, excepciones de dominio
    infrastructure/
      persistence/  -> OrderEntity, OrderRepository
      inventory/    -> InventoryClient, InventoryGateway (resiliencia + traducción de errores)
      messaging/    -> outbox y publicación en Kafka (fase 9)
  config/
  ```
  El código generado va en `<paquete raíz>.generated.api` y `.generated.dto` (en `target/`, nunca versionado ni editado).
- `package-info.java` con `@NullMarked` (JSpecify) en cada paquete propio.
- Inyección por constructor. Nada de `@Autowired` en campos ni de mappers con `INSTANCE` estático.
- Nunca bloquear: prohibido `.block()`, `Thread.sleep` y JDBC fuera de Liquibase.
- Fechas con `Instant` / `OffsetDateTime` en UTC. Nunca fechas como `String`.
- Errores HTTP con `ProblemDetail` (RFC 9457), con los mismos campos y extensiones que Inventory.
- Configuración en `application.yml` y perfiles `local`, `test`, `docker`, `k8s`. Secretos solo por variables de entorno (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`). URL de Inventory en `INVENTORY_BASE_URL`.
- Prefijo HTTP: `spring.webflux.base-path=/services-order`. Puerto 8081.
- `spring.application.name=service-order`.
- Mensajes de log y de error de API en español. Código y commits en inglés (Conventional Commits).

## Fuentes de verdad
- Contrato propio: `contracts/services-order.yaml` en la raíz del repo (v1.1.0). No hay copia en `src/main/resources`.
- Contrato consumido: `contracts/services-inventory.yaml` (el de Inventory; v1.2.0 desde la fase 5).
- Estados de la orden: `pending`, `completed`, `canceled` (en minúscula, tal como están en el contrato y en la BD).

## Reglas de Liquibase
- Un changeSet aplicado **no se edita nunca**. Cada cambio es un changeSet nuevo, con su `rollback`.
- Un solo changelog: `src/main/resources/db/changelog`. La CLI lo usa con `searchPath`; no existe una segunda copia.

## Forma de trabajar
1. Antes de tocar código, lista el plan de la fase y los archivos que vas a crear o modificar.
2. Trabaja **solo** en el alcance de la fase pedida.
3. Si una versión, artefacto u opción no coincide con lo que dice el prompt, **verifícalo** (guía de migración de Spring Boot 4, documentación del plugin, `./mvnw dependency:tree`) y explica el ajuste.
4. Al terminar: `./mvnw -q verify` y todos los criterios de aceptación de la fase.
5. Cierra con un resumen corto: archivos tocados, decisiones y cómo probarlo a mano.
