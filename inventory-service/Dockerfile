# syntax=docker/dockerfile:1.7
# Build from the course root so the OpenAPI contract is available as a named context:
#   docker build --build-context contracts=contracts -t codearti/service-inventory:0.0.1-SNAPSHOT inventory-service
# docker compose passes the same context through build.additional_contexts.

ARG JDK_IMAGE=eclipse-temurin:25-jdk@sha256:8c0a84ea11c8f6ed52600fc19f1040121f2a162998e9f50a5faebbbad9172dcc
ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian13@sha256:159783207c2cd44c2aa5715961d13c8612368ac9bd450f887e3f08fc8ea461e3

# --- build: compile and package without tests (CI runs ./mvnw verify separately) ---
FROM ${JDK_IMAGE} AS build
WORKDIR /workspace/inventory-service
COPY --from=contracts services-inventory.yaml /workspace/contracts/services-inventory.yaml
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
COPY src/ src/
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q package -DskipTests
# Layers keep plain jars (no --launcher): the AOT cache only archives classes loaded from jar files
# on the class path, not from exploded directories or the Spring Boot launcher class loader.
RUN java -Djarmode=tools -jar target/service-inventory-*.jar extract --layers \
        --application-filename app.jar --destination /workspace/extracted
COPY docker/HealthCheck.java /workspace/healthcheck/
RUN javac --release 25 -d /workspace/healthcheck /workspace/healthcheck/HealthCheck.java

# --- jre: minimal runtime with only the modules the service needs (jdeps + reflective extras) ---
FROM ${JDK_IMAGE} AS jre
RUN jlink --add-modules java.base,java.compiler,java.desktop,java.instrument,java.logging,java.management,java.naming,java.net.http,java.prefs,java.scripting,java.security.jgss,java.sql,java.xml,jdk.crypto.ec,jdk.jfr,jdk.management,jdk.naming.dns,jdk.unsupported,jdk.zipfs \
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
# The context is refreshed and closed without a database: Flyway is skipped and no connection is opened.
# Module options (JDK_JAVA_OPTIONS, set in the app stage) must match at runtime or the JVM silently drops the cache.
FROM app AS aot
RUN ["java", "-XX:AOTCacheOutput=/app/app.aot", "-Dspring.context.exit=onRefresh", "-Dspring.flyway.enabled=false", "-jar", "/app/app.jar"]

# --- runtime ---
FROM app
COPY --from=aot /app/app.aot /app/app.aot
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-XX:AOTCache=/app/app.aot", "-jar", "/app/app.jar"]
