package com.geovannycode.order.order.infrastructure.inventory;

import com.geovannycode.order.generated.inventory.api.InventoriesApi;
import io.netty.channel.ChannelOption;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.support.WebClientHttpServiceGroupConfigurer;
import org.springframework.web.service.registry.HttpServiceGroup;
import org.springframework.web.service.registry.ImportHttpServices;
import reactor.netty.http.client.HttpClient;

/**
 * Declarative HTTP service client (Spring Framework 7 / Boot 4.1): the InventoriesApi proxy is built on Boot's
 * WebClient.Builder, so WebClientCustomizers (observation: traces and metrics) still apply to it.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(InventoryClientProperties.class)
@ImportHttpServices(group = InventoryClientConfiguration.GROUP, types = InventoriesApi.class,
        clientType = HttpServiceGroup.ClientType.WEB_CLIENT)
public class InventoryClientConfiguration {

    static final String GROUP = "inventory";

    @Bean
    WebClientHttpServiceGroupConfigurer inventoryClientGroupConfigurer(InventoryClientProperties properties) {
        return new InventoryGroupConfigurer(properties);
    }

    /** Runs after Boot's configurers, so inventory.client is the single source of URL and timeouts. */
    private record InventoryGroupConfigurer(InventoryClientProperties properties) implements WebClientHttpServiceGroupConfigurer {

        @Override
        public void configureGroups(Groups<WebClient.Builder> groups) {
            var httpClient = HttpClient.create()
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(properties.connectTimeout().toMillis()))
                    .responseTimeout(properties.responseTimeout());
            groups.filterByName(GROUP).forEachClient((group, builder) -> builder
                    .baseUrl(properties.baseUrl().toString())
                    .clientConnector(new ReactorClientHttpConnector(httpClient)));
        }

        @Override
        public int getOrder() {
            return Ordered.LOWEST_PRECEDENCE;
        }
    }
}
