package com.geovannycode.inventory.config;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;

@Configuration(proxyBeanMethods = false)
public final class ReactiveTransactionConfiguration {

    @Bean
    R2dbcTransactionManager inventoryTransactionManager(ConnectionFactory connectionFactory) {
        return new R2dbcTransactionManager(connectionFactory);
    }
}
