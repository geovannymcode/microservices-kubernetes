package com.geovannycode.notify_service.config;

import com.geovannycode.notify_service.notify.infrastructure.persistence.NotifyStatusConverters;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.EnableReactiveMongoAuditing;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;

@Configuration(proxyBeanMethods = false)
@EnableReactiveMongoAuditing
public class PersistenceConfiguration {

    // Replaces Boot's default (empty) MongoCustomConversions; the store's built-in conversions are kept.
    @Bean
    MongoCustomConversions mongoCustomConversions() {
        return new MongoCustomConversions(NotifyStatusConverters.all());
    }
}
