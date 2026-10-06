package com.geovannycode.notify_service.notify.infrastructure.sender;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

// Bound and validated whatever the channel, so a typo in notification.sender.type fails at startup too.
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(NotificationSenderProperties.class)
class NotificationSenderConfiguration {
}
