package com.geovannycode.notify_service.config;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class ClockConfiguration {

    // Injected instead of Instant.now() so tests can fix sentAt.
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
