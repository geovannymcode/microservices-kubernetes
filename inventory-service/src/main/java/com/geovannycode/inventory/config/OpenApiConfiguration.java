package com.geovannycode.inventory.config;

import java.io.IOException;
import java.io.InputStream;

import io.swagger.v3.core.util.Yaml31;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

@Configuration(proxyBeanMethods = false)
public final class OpenApiConfiguration {

    // title, version and description come from the contract packaged at build time, never from code.
    @Bean
    OpenAPI inventoryOpenApi(@Value("${spring.webflux.base-path}") String basePath,
                             @Value("classpath:contract/services-inventory.yaml") Resource contract) throws IOException {
        try (InputStream yaml = contract.getInputStream()) {
            var info = Yaml31.mapper().readValue(yaml, OpenAPI.class).getInfo();
            return new OpenAPI().info(info)
                    .addServersItem(new Server().url(basePath).description("Prefijo del servicio"));
        }
    }
}
