package com.geovannycode.inventory.config;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.web.cors.reactive.CorsWebFilter;

import static org.assertj.core.api.Assertions.assertThat;

final class LocalCorsConfigurationTest {

    @Test
    void corsIsNotEnabledOutsideLocalProfile() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().setActiveProfiles("k8s");
            context.register(LocalCorsConfiguration.class);
            context.refresh();
            assertThat(context.getBeansOfType(CorsWebFilter.class)).isEmpty();
        }
    }
}
