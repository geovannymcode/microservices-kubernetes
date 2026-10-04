package com.geovannycode.order;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;

// One container for JDBC (Liquibase) and R2DBC (the app): @ServiceConnection derives both connection details.
@TestConfiguration(proxyBeanMethods = false)
public final class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        // postgres:17.11, same digest as docker-compose.yaml. Testcontainers rejects "name:tag@digest", so no tag.
        return new PostgreSQLContainer(
                "postgres@sha256:d74eeac9a635390a49bc21bd49fccd973de707e2a53a76ac49b552b8712ec46f");
    }
}
