package com.geovannycode.notify_service;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
final class NotifyServiceApplicationTest {

    @Test
    void contextLoads() {
    }
}
