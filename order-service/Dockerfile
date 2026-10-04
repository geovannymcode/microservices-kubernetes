# syntax=docker/dockerfile:1.7
# Same strategy as service-inventory (ADR 0002). Build from the course root so the OpenAPI contracts (server
# and the Inventory client) are available as a named context, without copying contracts/ into the module:
#   docker build --build-context contracts=contracts -t geovannycode/service-order:0.0.1-SNAPSHOT order-service
# docker compose passes the same context through build.additional_contexts.

ARG JDK_IMAGE=eclipse-temurin:25-jdk@sha256:8c0a84ea11c8f6ed52600fc19f1040121f2a162998e9f50a5faebbbad9172dcc
ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian13@sha256:159783207c2cd44c2aa5715961d13c8612368ac9bd450f887e3f08fc8ea461e3

# --- build: compile and package without tests (CI runs ./mvnw verify separately) ---
FROM ${JDK_IMAGE} AS build
# mvnw only downloads the .zip whose SHA-256 is pinned in maven-wrapper.properties when unzip exists; without it
# (the temurin image has none) it switches to the .tar.gz and the checksum fails. Build stage only.
RUN apt-get update && apt-get install -y --no-install-recommends unzip && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace/order-service
COPY --from=contracts services-order.yaml services-inventory.yaml /workspace/contracts/
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
COPY src/ src/
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q package -DskipTests
# Layers keep plain jars (no --launcher): the AOT cache only archives classes loaded from jar files
# on the class path, not from exploded directories or the Spring Boot launcher class loader.
RUN java -Djarmode=tools -jar target/order-service-*.jar extract --layers \
        --application-filename app.jar --destination /workspace/extracted
COPY docker/HealthCheck.java /workspace/healthcheck/
RUN javac --release 25 -d /workspace/healthcheck /workspace/healthcheck/HealthCheck.java

# --- jre: minimal runtime with only the modules the service needs (jdeps + reflective extras) ---
# Same set as service-inventory plus java.security.sasl, which the Kafka client links against.
FROM ${JDK_IMAGE} AS jre
RUN jlink --add-modules java.base,java.compiler,java.desktop,java.instrument,java.logging,java.management,java.naming,java.net.http,java.prefs,java.scripting,java.security.jgss,java.security.sasl,java.sql,java.xml,jdk.crypto.ec,jdk.jfr,jdk.management,jdk.naming.dns,jdk.unsupported,jdk.zipfs \
        --strip-debug --no-man-pages --no-header-files --compress=zip-6 --output /opt/jre

# --- app: distroless base + JRE + application layers (least to most frequently changing) ---
FROM ${RUNTIME_IMAGE} AS app
ENV JAVA_HOME=/opt/java \
    PATH=/opt/java/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError" \
    JDK_JAVA_OPTIONS="--enable-native-access=ALL-UNNAMED"
COPY --from=jre /opt/jre /opt/java
WORKDIR /app
COPY --from=build /workspace/extracted/dependencies/ ./
COPY --from=build /workspace/extracted/spring-boot-loader/ ./
COPY --from=build /workspace/extracted/snapshot-dependencies/ ./
COPY --from=build /workspace/extracted/application/ ./
COPY --from=build /workspace/healthcheck/HealthCheck.class ./healthcheck/

# --- aot: training run on the exact runtime JVM and class path (JEP 483/514/515) ---
# Needs no PostgreSQL, Kafka or Inventory: the context is refreshed and the JVM exits before lifecycle beans
# start (no Netty server, no outbox relay, no Kafka producer), Liquibase is skipped, the R2DBC pool opens no
# connection until first use, and without an active profile there is no NewTopic for KafkaAdmin to create.
# Module options (JDK_JAVA_OPTIONS, set in the app stage) must match at runtime or the JVM silently drops the cache.
FROM app AS aot
# BuildKit's docker-container driver (CI's setup-buildx) injects OTEL_TRACES_EXPORTER=otlp and a unix:// endpoint
# into RUN steps for its own tracing; Boot reads OTEL_* with priority over any property and the OTLP exporter
# rejects that endpoint. A Dockerfile ENV wins over the injected one, and this stage never reaches the runtime image.
ENV OTEL_TRACES_EXPORTER=none
RUN ["java", "-XX:AOTCacheOutput=/app/app.aot", "-Dspring.context.exit=onRefresh", "-Dspring.liquibase.enabled=false", "-jar", "/app/app.jar"]

# --- runtime ---
FROM app
COPY --from=aot /app/app.aot /app/app.aot
USER 10001:10001
EXPOSE 8081
ENTRYPOINT ["java", "-XX:AOTCache=/app/app.aot", "-jar", "/app/app.jar"]
