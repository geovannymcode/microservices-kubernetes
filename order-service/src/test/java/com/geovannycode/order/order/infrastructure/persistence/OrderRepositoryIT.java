package com.geovannycode.order.order.infrastructure.persistence;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import com.geovannycode.order.TestcontainersConfiguration;
import com.geovannycode.order.config.PersistenceConfiguration;
import com.geovannycode.order.order.domain.CancelReason;
import com.geovannycode.order.order.domain.OrderStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.boot.liquibase.autoconfigure.LiquibaseAutoConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.auditing.CurrentDateTimeProvider;
import org.springframework.data.auditing.ReactiveIsNewAwareAuditingHandler;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@DataR2dbcTest
@Import({TestcontainersConfiguration.class, PersistenceConfiguration.class})
@ImportAutoConfiguration(LiquibaseAutoConfiguration.class)
@ActiveProfiles("test")
final class OrderRepositoryIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private final OrderRepository repository;
    private final DatabaseClient database;

    @Autowired
    OrderRepositoryIT(OrderRepository repository, DatabaseClient database) {
        this.repository = repository;
        this.database = database;
    }

    @BeforeEach
    void cleanTable() {
        StepVerifier.create(repository.deleteAll()).expectComplete().verify(TIMEOUT);
    }

    @Test
    void saveAssignsIdAuditDatesAndInitialVersion() {
        StepVerifier.create(repository.save(OrderEntity.pending("AC-1550", 2)))
                .assertNext(saved -> {
                    assertThat(saved.id()).isNotNull();
                    assertThat(saved.createdAt()).isNotNull();
                    assertThat(saved.updatedAt()).isNotNull();
                    assertThat(saved.version()).isZero();
                    assertThat(saved.status()).isEqualTo(OrderStatus.PENDING);
                })
                .expectComplete().verify(TIMEOUT);
    }

    @Test
    void updatingIncrementsVersionAndRefreshesUpdatedAt(
            @Autowired ReactiveIsNewAwareAuditingHandler auditingHandler) {
        // Force nanoseconds on every OS and advance audit time without sleeping or relying on wall-clock resolution.
        var initialTime = Instant.parse("2026-10-06T01:03:47.023756238Z");
        var auditTick = new AtomicLong();
        auditingHandler.setDateTimeProvider(() -> Optional.of(initialTime.plusSeconds(auditTick.getAndIncrement())));
        try {
            var original = repository.save(OrderEntity.pending("AC-1550", 1))
                    .flatMap(saved -> repository.findById(saved.id()));
            // Compare persisted snapshots: save() retains Java nanoseconds, PostgreSQL stores microseconds.
            StepVerifier.create(original.flatMap(stored -> repository.save(stored.complete())
                            .flatMap(updated -> repository.findById(updated.id()))
                            .map(reloaded -> new OrderEntity[]{stored, reloaded})))
                    .assertNext(pair -> {
                        var stored = pair[0];
                        var reloaded = pair[1];
                        assertThat(reloaded.version()).isEqualTo(1L);
                        assertThat(reloaded.status()).isEqualTo(OrderStatus.COMPLETED);
                        assertThat(reloaded.updatedAt()).isAfter(stored.updatedAt());
                        assertThat(reloaded.createdAt()).isEqualTo(stored.createdAt());
                    })
                    .expectComplete().verify(TIMEOUT);
        } finally {
            auditingHandler.setDateTimeProvider(CurrentDateTimeProvider.INSTANCE);
        }
    }

    @Test
    void secondWriteOnTheSameVersionFailsWithOptimisticLocking() {
        StepVerifier.create(repository.save(OrderEntity.pending("AC-1550", 1))
                        .flatMap(stale -> repository.save(stale.complete())
                                .then(repository.save(stale.cancel(CancelReason.INSUFFICIENT_STOCK)))))
                .expectError(OptimisticLockingFailureException.class)
                .verify(TIMEOUT);
    }

    @Test
    void findsByStatusInIdOrder() {
        var setup = Flux.concat(
                repository.save(OrderEntity.pending("AC-1550", 1)),
                repository.save(OrderEntity.pending("AC-1551", 1)).flatMap(order -> repository.save(order.complete())),
                repository.save(OrderEntity.pending("AC-1552", 1)));
        StepVerifier.create(setup.thenMany(repository.findAllByStatusOrderByIdAsc(OrderStatus.PENDING))
                        .map(OrderEntity::codeProduct).collectList())
                .assertNext(codes -> assertThat(codes).containsExactly("AC-1550", "AC-1552"))
                .expectComplete().verify(TIMEOUT);
        StepVerifier.create(repository.findAllByStatusOrderByIdAsc(OrderStatus.COMPLETED).map(OrderEntity::codeProduct))
                .expectNext("AC-1551").expectComplete().verify(TIMEOUT);
        StepVerifier.create(repository.findAllByOrderByIdAsc().map(OrderEntity::codeProduct).collectList())
                .assertNext(codes -> assertThat(codes).containsExactly("AC-1550", "AC-1551", "AC-1552"))
                .expectComplete().verify(TIMEOUT);
    }

    @Test
    void storesStatusAsLowercaseValueAndCancelReasonByName() {
        var canceled = repository.save(OrderEntity.pending("AC-1550", 1))
                .flatMap(order -> repository.save(order.cancel(CancelReason.PRODUCT_NOT_FOUND)));
        StepVerifier.create(canceled.flatMap(order -> database
                        .sql("SELECT status_order, cancel_reason FROM order_shop WHERE id = :id")
                        .bind("id", order.id())
                        .map(row -> row.get("status_order", String.class) + "/" + row.get("cancel_reason", String.class))
                        .one()))
                .expectNext("canceled/PRODUCT_NOT_FOUND")
                .expectComplete().verify(TIMEOUT);
    }
}
