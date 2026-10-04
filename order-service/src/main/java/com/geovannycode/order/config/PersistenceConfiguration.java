package com.geovannycode.order.config;

import com.geovannycode.order.order.infrastructure.persistence.OrderStatusConverters;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.config.EnableR2dbcAuditing;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.r2dbc.core.DatabaseClient;

@Configuration(proxyBeanMethods = false)
@EnableR2dbcAuditing
public class PersistenceConfiguration {

    // Replaces Boot's default conversions bean: same dialect-aware store conversions plus OrderStatus <-> value.
    @Bean
    R2dbcCustomConversions r2dbcCustomConversions(DatabaseClient databaseClient) {
        var dialect = DialectResolver.getDialect(databaseClient.getConnectionFactory());
        return R2dbcCustomConversions.of(dialect, OrderStatusConverters.all());
    }
}
