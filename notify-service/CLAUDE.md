# CLAUDE.md — service-notify

## Proyecto
Microservicio **Notification Service** del curso "Arquitectura de Microservicios con Spring Boot y Kubernetes". Consume los eventos de órdenes que Order publica en Kafka, registra una notificación por evento en MongoDB, la envía por un canal y expone una API de solo lectura.

**Se construye desde cero** a partir de un esqueleto de Spring Initializr. No hay código previo que migrar.

Inventory y Order ya están implementados en este mismo repo. Úsalos como referencia de estilo: mismas convenciones, mismo formato de errores, misma estructura de tests, compose y manifiestos. Order es la referencia más cercana (API-first con delegate).

**Nombres:** los prompts dicen `service-notify`, `service-order` y `service-inventory`. Si en el repo las carpetas, el paquete raíz o los nombres de servicio son otros, usa los que ya existen y mantenlos coherentes con los otros dos servicios. No renombres nada.

## Stack obligatorio
- Java **25 LTS**, Maven Wrapper 3.9.x
- Spring Boot **4.1.x** (Spring Framework 7, Jackson 3 `tools.jackson.*`, starters modulares)
- WebFlux + Spring Data MongoDB **reactivo**
- MongoDB **8.0**
- `spring-kafka` con `@KafkaListener`. **No uses reactor-kafka** (descontinuado).
- **API-first**: `openapi-generator-maven-plugin` con delegate, desde `contracts/services-notify.yaml`
- **Sin Lombok.** Documentos y DTO como `record`.
- Tests: JUnit 5, `StepVerifier`, `WebTestClient`, Testcontainers 2.x (MongoDB y Kafka) con `@ServiceConnection`, Awaitility

## Convenciones
- Paquete raíz: el del esqueleto (`com.geovannycode.notify_service`):
  ```
  notify/
    api/            -> NotifyApiDelegateImpl, GlobalExceptionHandler, NotifyMapper
    application/    -> NotificationService (casos de uso)
    domain/         -> NotifyStatus, OrderEvent, NotificationSender (puerto), excepciones
    infrastructure/
      persistence/  -> NotificationDocument, NotificationRepository, índices
      messaging/    -> OrderEventListener, configuración de Kafka y manejo de errores
      sender/       -> LogNotificationSender, WebhookNotificationSender
  config/
  ```
  El código generado va en `<paquete raíz>.generated.api` y `.generated.dto` (en `target/`, nunca versionado ni editado).
- `package-info.java` con `@NullMarked` (JSpecify) en cada paquete propio.
- Inyección por constructor.
- **No bloquear**, con UNA excepción documentada: dentro del método `@KafkaListener` (hilo del consumidor de Kafka, no de WebFlux) se espera el resultado de la cadena reactiva con `block(Duration)`. En ningún otro sitio.
- Fechas con `Instant` en UTC. Nunca fechas como `String`.
- Errores HTTP con `ProblemDetail` (RFC 9457), con los mismos campos que Inventory y Order.
- Configuración en `application.yml` y perfiles `local`, `test`, `docker`, `k8s`. Secretos solo por variables de entorno: `MONGO_HOST`, `MONGO_PORT`, `MONGO_DB`, `MONGO_USER`, `MONGO_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS`.
- Prefijo HTTP: `spring.webflux.base-path=/services-notify`. Puerto 8082.
- `spring.application.name=service-notify`. Consumer group: `service-notify`.
- Mensajes de log, de error de API y texto de las notificaciones en español. Código y commits en inglés (Conventional Commits).

## Fuentes de verdad
- Contrato de la API: `contracts/services-notify.yaml` en la raíz del repo (v2.0.0). No hay copia en `src/main/resources`.
- Contrato del evento: `contracts/events/order-events.yaml` (AsyncAPI, lo creó la fase 9 de Order) y el código del productor en Order. **Si este CLAUDE.md o un prompt difieren de lo que Order publica de verdad, manda lo que publica Order**: ajústate y avisa.
- Evento esperado: topic `orders.events.v1`, key = id de la orden, headers `eventType` y `eventId`, cuerpo JSON `{ eventId, eventType, occurredAt, version, data { orderId, codeProduct, quantity, status, cancelReason? } }`. Entrega al menos una vez: puede llegar repetido.
- Estados de la notificación: `pending`, `sent`, `failed` (en minúscula en la API y en Mongo).
- Colección Mongo: `notify_orders`. Base: `db_notify`.

## Forma de trabajar
1. Antes de tocar código, lista el plan de la fase y los archivos que vas a crear o modificar.
2. Trabaja **solo** en el alcance de la fase pedida.
3. Si una versión, artefacto u opción no coincide con lo que dice el prompt, **verifícalo** (guía de migración de Spring Boot 4, documentación de spring-kafka, `./mvnw dependency:tree`) y explica el ajuste.
4. Al terminar: `./mvnw -q verify` y todos los criterios de aceptación de la fase.
5. Cierra con un resumen corto: archivos tocados, decisiones y cómo probarlo a mano.
