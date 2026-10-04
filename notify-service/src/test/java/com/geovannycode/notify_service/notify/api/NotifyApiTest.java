package com.geovannycode.notify_service.notify.api;

import java.lang.reflect.Method;

import com.geovannycode.notify_service.generated.api.NotifyApiDelegate;
import com.geovannycode.notify_service.generated.config.EnumConverterConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.assertj.core.api.Assertions.assertThat;

// The slice client does not go through the server's base path (/services-notify).
@WebFluxTest(properties = "spring.webflux.base-path=")
// The generated EnumConverterConfiguration is a @Configuration, which slices do not pick up on their own.
@Import({NotifyApiDelegateImpl.class, EnumConverterConfiguration.class})
final class NotifyApiTest {

    @Autowired private WebTestClient client;

    @Test
    void overridesEveryGeneratedOperation() throws NoSuchMethodException {
        // skipDefaultInterface must stay false with delegatePattern, so the compiler cannot enforce this.
        for (Method operation : NotifyApiDelegate.class.getMethods()) {
            if (operation.isDefault() && !operation.getName().equals("getRequest")) {
                var implementation = NotifyApiDelegateImpl.class.getMethod(operation.getName(), operation.getParameterTypes());
                assertThat(implementation.getDeclaringClass()).as(operation.getName()).isEqualTo(NotifyApiDelegateImpl.class);
            }
        }
    }

    @Test
    void operationsAreNotImplementedYet() {
        client.get().uri("/notify").exchange().expectStatus().isEqualTo(501);
        client.get().uri("/notify?orderId=42&status=sent&limit=10").exchange().expectStatus().isEqualTo(501);
        client.get().uri("/notify/66ff1c2ab7e4d91a2c3d4e5f").exchange().expectStatus().isEqualTo(501);
    }

    @Test
    void generatedValidationRejectsOutOfRangeParameters() {
        problem(client.get().uri("/notify?limit=9999").exchange(), "validation-error")
                .jsonPath("$.errors[0].field").isEqualTo("limit");
        problem(client.get().uri("/notify?orderId=0").exchange(), "validation-error")
                .jsonPath("$.errors[0].field").isEqualTo("orderId");
        problem(client.get().uri("/notify/no-es-un-id").exchange(), "validation-error")
                .jsonPath("$.errors[0].field").isEqualTo("notifyId");
        problem(client.get().uri("/notify?status=otro").exchange(), "invalid-request");
    }

    private static WebTestClient.BodyContentSpec problem(WebTestClient.ResponseSpec response, String slug) {
        return response.expectStatus().isBadRequest()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.type").isEqualTo("https://geovannycode.com/problems/" + slug)
                .jsonPath("$.status").isEqualTo(400).jsonPath("$.title").exists().jsonPath("$.timestamp").exists();
    }
}
