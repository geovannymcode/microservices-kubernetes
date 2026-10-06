package com.geovannycode.notify_service.notify.infrastructure.sender;

import com.geovannycode.notify_service.notify.domain.NotificationSender;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration;
import org.springframework.boot.webclient.autoconfigure.WebClientAutoConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

// Channel selection and startup validation, without containers: exactly one NotificationSender, or a clear failure.
final class NotificationSenderConfigurationTest {

    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
                    ValidationAutoConfiguration.class, JacksonAutoConfiguration.class, WebClientAutoConfiguration.class))
            .withUserConfiguration(NotificationSenderConfiguration.class, LogNotificationSender.class,
                    WebhookNotificationSender.class)
            // The defaults application.yml provides.
            .withPropertyValues("notification.sender.webhook.connect-timeout=1s",
                    "notification.sender.webhook.response-timeout=3s");

    @Test
    void logIsTheDefaultChannel() {
        context.run(app -> {
            assertThat(app).hasSingleBean(NotificationSender.class).hasSingleBean(LogNotificationSender.class);
            assertThat(app.getBean(NotificationSender.class).channel()).isEqualTo("log");
        });
    }

    @Test
    void webhookWithUrlReplacesTheLogChannel() {
        context.withPropertyValues("notification.sender.type=webhook",
                "notification.sender.webhook.url=https://hooks.example.com/notify?token=abc").run(app -> {
            assertThat(app).hasSingleBean(NotificationSender.class).hasSingleBean(WebhookNotificationSender.class);
            assertThat(app.getBean(NotificationSender.class).channel()).isEqualTo("webhook");
        });
    }

    @Test
    void webhookWithoutUrlDoesNotStartAndSaysWhy() {
        context.withPropertyValues("notification.sender.type=webhook", "notification.sender.webhook.url=").run(app ->
                assertThat(app).hasFailed().getFailure().rootCause().hasMessageContaining("NOTIFY_WEBHOOK_URL"));
    }

    @Test
    void webhookWithAUrlThatIsNotHttpDoesNotStart() {
        context.withPropertyValues("notification.sender.type=webhook", "notification.sender.webhook.url=ftp://x/y").run(app ->
                assertThat(app).hasFailed().getFailure().rootCause().hasMessageContaining("NOTIFY_WEBHOOK_URL"));
    }

    @Test
    void unknownChannelDoesNotStart() {
        context.withPropertyValues("notification.sender.type=smtp").run(app -> assertThat(app).hasFailed());
    }

    @Test
    void webhookSettingsNeverPrintTheUrlOrTheSecret() {
        var webhook = new NotificationSenderProperties.Webhook("https://h.example.com/x?token=t0k3n",
                java.time.Duration.ofSeconds(1), java.time.Duration.ofSeconds(3), "s3cr3t");
        assertThat(webhook.toString()).doesNotContain("t0k3n").doesNotContain("s3cr3t");
    }
}
