package com.geovannycode.inventory.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public final class OpenApiConfiguration {

    @Bean
    OpenAPI inventoryOpenApi(@Value("${spring.webflux.base-path}") String basePath) {
        return new OpenAPI()
                .info(new Info().title("Inventory Service").version("1.1.0")
                        .description("Consulta, creación y descuento atómico de existencias."))
                .addServersItem(new Server().url(basePath).description("Prefijo del servicio"));
    }
}
