package com.geovannycode.notify_service.config;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.geovannycode.notify_service.notify.infrastructure.messaging.OrderEventListener;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Sends every log record to the OpenTelemetry SDK, which exports it by OTLP (to Loki through the collector) with
 * its trace and span ids. Attached in code instead of a logback-spring.xml, so Boot keeps owning the console
 * output (plain locally, ECS JSON in docker and k8s). Whether records actually leave is decided by
 * management.logging.export.otlp.enabled (OTEL_LOGS_EXPORTER).
 */
@Configuration(proxyBeanMethods = false)
class OpenTelemetryLogsConfiguration {

    private static final String APPENDER = "OTEL";

    @Bean
    InitializingBean openTelemetryLogAppender(OpenTelemetry openTelemetry) {
        return () -> {
            var context = (LoggerContext) LoggerFactory.getILoggerFactory();
            Logger root = context.getLogger(Logger.ROOT_LOGGER_NAME);
            // Boot resets Logback when a new application starts in the same JVM (tests): attach once per reset.
            if (root.getAppender(APPENDER) == null) {
                var appender = new OpenTelemetryAppender();
                appender.setName(APPENDER);
                appender.setContext(context);
                appender.setCaptureMdcAttributes(OrderEventListener.MDC_EVENT_ID + "," + OrderEventListener.MDC_ORDER_ID);
                appender.setCaptureKeyValuePairAttributes(true);
                appender.start();
                root.addAppender(appender);
            }
            OpenTelemetryAppender.install(openTelemetry);
        };
    }
}
