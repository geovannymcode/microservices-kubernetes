package com.geovannycode.order.config;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.web.cors.reactive.CorsWebFilter;

import static org.assertj.core.api.Assertions.assertThat;

final class LocalCorsConfigurationTest {

    @Test
    void corsIsOnlyEnabledInTheLocalProfile() {
        for (String profile : new String[]{"local", "k8s"}) {
            try (var context = new AnnotationConfigApplicationContext()) {
                context.getEnvironment().setActiveProfiles(profile);
                context.register(LocalCorsConfiguration.class);
                context.refresh();
                assertThat(context.getBeansOfType(CorsWebFilter.class)).as(profile).hasSize(profile.equals("local") ? 1 : 0);
            }
        }
    }
}
