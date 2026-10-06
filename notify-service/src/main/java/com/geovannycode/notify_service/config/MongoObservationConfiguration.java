package com.geovannycode.notify_service.config;

import io.micrometer.observation.ObservationRegistry;
import org.springframework.boot.mongodb.autoconfigure.MongoClientSettingsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.observability.ContextProviderFactory;
import org.springframework.data.mongodb.observability.MongoObservationCommandListener;

/**
 * One span per MongoDB command, child of the current observation (the Kafka consumer span or the HTTP request).
 * Boot only auto-configures MongoDB metrics, not traces. With the reactive driver the parent travels in the
 * Reactor context, which the context provider reads (spring.reactor.context-propagation=auto fills it).
 */
@Configuration(proxyBeanMethods = false)
class MongoObservationConfiguration {

    @Bean
    MongoClientSettingsBuilderCustomizer mongoObservation(ObservationRegistry observationRegistry) {
        return settings -> settings.contextProvider(ContextProviderFactory.create(observationRegistry))
                .addCommandListener(new MongoObservationCommandListener(observationRegistry));
    }
}
