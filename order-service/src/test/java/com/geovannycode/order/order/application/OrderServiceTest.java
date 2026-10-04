package com.geovannycode.order.order.application;

import java.time.Instant;

import com.geovannycode.order.generated.dto.OrderRequest;
import com.geovannycode.order.order.domain.CancelReason;
import com.geovannycode.order.order.domain.IllegalOrderStateException;
import com.geovannycode.order.order.domain.InventoryRejectedException;
import com.geovannycode.order.order.domain.InventoryUnavailableException;
import com.geovannycode.order.order.domain.OrderNotFoundException;
import com.geovannycode.order.order.domain.OrderRejectedException;
import com.geovannycode.order.order.domain.OrderStatus;
import com.geovannycode.order.order.infrastructure.inventory.InventoryGateway;
import com.geovannycode.order.order.infrastructure.messaging.OutboxWriter;
import com.geovannycode.order.order.infrastructure.persistence.OrderEntity;
import com.geovannycode.order.order.infrastructure.persistence.OrderRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
final class OrderServiceTest {

    private static final long ID = 10L;
    private static final Instant NOW = Instant.parse("2026-10-03T19:00:00Z");

    @Mock private OrderRepository repository;
    @Mock private InventoryGateway inventory;
    @Mock private OutboxWriter outbox;
    @Mock private TransactionalOperator transactions;
    private OrderService service;
    private SimpleMeterRegistry meters;

    @BeforeEach
    void createService() {
        // Pass-through transaction; OutboxIT covers the real commit and rollback against PostgreSQL.
        lenient().when(transactions.transactional(any(Mono.class))).thenAnswer(call -> call.getArgument(0));
        lenient().when(outbox.append(any(OrderEntity.class))).thenReturn(Mono.empty());
        meters = new SimpleMeterRegistry();
        service = new OrderService(repository, inventory, new OrderMapperImpl(), outbox, transactions, meters);
    }

    private static OrderEntity order(OrderStatus status) {
        return new OrderEntity(ID, "AC-1550", 2, status,
                status == OrderStatus.CANCELED ? CancelReason.INSUFFICIENT_STOCK : null, NOW, NOW, 0L);
    }

    private double confirmed(String result) {
        return meters.get("orders.confirmed").tag("result", result).counter().count();
    }

    private void saveReturnsArgument() {
        when(repository.save(any(OrderEntity.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));
    }

    @Test
    void createStoresPendingOrderWithoutCallingInventory() {
        saveReturnsArgument();
        StepVerifier.create(service.create(new OrderRequest("ac-1550").quantity(3)))
                .assertNext(response -> {
                    assertThat(response.getStatus().getValue()).isEqualTo("pending");
                    assertThat(response.getCodeProduct()).isEqualTo("AC-1550");
                    assertThat(response.getQuantity()).isEqualTo(3);
                }).verifyComplete();
        verifyNoInteractions(inventory);
        assertThat(meters.get("orders.registered").counter().count()).isEqualTo(1);
    }

    @Test
    void missingOrderIsNotFoundAndInventoryIsNeverCalled() {
        when(repository.findById(ID)).thenReturn(Mono.empty());
        StepVerifier.create(service.findById(ID)).expectError(OrderNotFoundException.class).verify();
        StepVerifier.create(service.confirm(ID)).expectError(OrderNotFoundException.class).verify();
        verifyNoInteractions(inventory);
    }

    @Test
    void findByIdReturnsTheMappedOrder() {
        when(repository.findById(ID)).thenReturn(Mono.just(order(OrderStatus.CANCELED)));
        StepVerifier.create(service.findById(ID))
                .assertNext(response -> {
                    assertThat(response.getId()).isEqualTo(ID);
                    assertThat(response.getStatus().getValue()).isEqualTo("canceled");
                    assertThat(response.getCancelReason().getValue()).isEqualTo("INSUFFICIENT_STOCK");
                    assertThat(response.getCreatedAt().getOffset()).isEqualTo(java.time.ZoneOffset.UTC);
                }).verifyComplete();
    }

    @Test
    void findAllFiltersByStatusOnlyWhenGiven() {
        when(repository.findAllByOrderByIdAsc()).thenReturn(Flux.just(order(OrderStatus.PENDING), order(OrderStatus.COMPLETED)));
        when(repository.findAllByStatusOrderByIdAsc(OrderStatus.COMPLETED)).thenReturn(Flux.just(order(OrderStatus.COMPLETED)));
        StepVerifier.create(service.findAll(null)).expectNextCount(2).verifyComplete();
        StepVerifier.create(service.findAll(OrderStatus.COMPLETED)).expectNextCount(1).verifyComplete();
    }

    @Test
    void completedOrderIsReturnedWithoutCallingInventory() {
        when(repository.findById(ID)).thenReturn(Mono.just(order(OrderStatus.COMPLETED)));
        StepVerifier.create(service.confirm(ID))
                .assertNext(response -> assertThat(response.getStatus().getValue()).isEqualTo("completed"))
                .verifyComplete();
        verifyNoInteractions(inventory, outbox);
        verify(repository, never()).save(any());
        // Re-confirming a completed order is not a new confirmation.
        assertThat(confirmed("completed")).isZero();
    }

    @Test
    void canceledOrderCannotBeConfirmed() {
        when(repository.findById(ID)).thenReturn(Mono.just(order(OrderStatus.CANCELED)));
        StepVerifier.create(service.confirm(ID)).expectError(IllegalOrderStateException.class).verify();
        verifyNoInteractions(inventory);
    }

    @Test
    void pendingOrderIsCompletedWhenInventoryDecreasesStock() {
        when(repository.findById(ID)).thenReturn(Mono.just(order(OrderStatus.PENDING)));
        when(inventory.reserveStock(ID, "AC-1550", 2)).thenReturn(Mono.empty());
        saveReturnsArgument();
        StepVerifier.create(service.confirm(ID))
                .assertNext(response -> assertThat(response.getStatus().getValue()).isEqualTo("completed"))
                .verifyComplete();

        var saved = ArgumentCaptor.forClass(OrderEntity.class);
        verify(repository).save(saved.capture());
        assertThat(saved.getValue().status()).isEqualTo(OrderStatus.COMPLETED);
        assertThat(saved.getValue().cancelReason()).isNull();
        verify(outbox).append(saved.getValue());
        assertThat(confirmed("completed")).isEqualTo(1);
    }

    @Test
    void rejectionPersistsTheCancellationAndAnswersWithTheReason() {
        when(repository.findById(ID)).thenReturn(Mono.just(order(OrderStatus.PENDING)));
        when(inventory.reserveStock(ID, "AC-1550", 2))
                .thenReturn(Mono.error(new InventoryRejectedException("AC-1550", CancelReason.PRODUCT_NOT_FOUND)));
        saveReturnsArgument();

        StepVerifier.create(service.confirm(ID))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(OrderRejectedException.class)
                        .extracting(e -> ((OrderRejectedException) e).reason()).isEqualTo(CancelReason.PRODUCT_NOT_FOUND))
                .verify();

        var saved = ArgumentCaptor.forClass(OrderEntity.class);
        verify(repository).save(saved.capture());
        assertThat(saved.getValue().status()).isEqualTo(OrderStatus.CANCELED);
        assertThat(saved.getValue().cancelReason()).isEqualTo(CancelReason.PRODUCT_NOT_FOUND);
        verify(outbox).append(saved.getValue());
        assertThat(confirmed("canceled")).isEqualTo(1);
        assertThat(confirmed("completed")).isZero();
    }

    @Test
    void unavailableInventoryLeavesTheOrderPending() {
        when(repository.findById(ID)).thenReturn(Mono.just(order(OrderStatus.PENDING)));
        when(inventory.reserveStock(ID, "AC-1550", 2))
                .thenReturn(Mono.error(new InventoryUnavailableException(new RuntimeException("down"))));
        StepVerifier.create(service.confirm(ID)).expectError(InventoryUnavailableException.class).verify();
        verify(repository, never()).save(any());
        verifyNoInteractions(outbox);
        assertThat(confirmed("unavailable")).isEqualTo(1);
    }

    @Test
    void concurrentConfirmationRereadsAndEndsCompleted() {
        // First read: still pending; our save loses the version race; the re-read sees the other request's result.
        when(repository.findById(ID)).thenReturn(Mono.just(order(OrderStatus.PENDING)), Mono.just(order(OrderStatus.COMPLETED)));
        when(inventory.reserveStock(ID, "AC-1550", 2)).thenReturn(Mono.empty());
        when(repository.save(any(OrderEntity.class))).thenReturn(Mono.error(new OptimisticLockingFailureException("version")));

        StepVerifier.create(service.confirm(ID))
                .assertNext(response -> assertThat(response.getStatus().getValue()).isEqualTo("completed"))
                .verifyComplete();
        verify(inventory, times(1)).reserveStock(anyLong(), anyString(), anyInt());
        // The losing save failed, so its event was never appended: the winner wrote the only one.
        verifyNoInteractions(outbox);
    }

    @Test
    void outboxFailureFailsTheConfirmationInsteadOfLosingTheEvent() {
        when(repository.findById(ID)).thenReturn(Mono.just(order(OrderStatus.PENDING)));
        when(inventory.reserveStock(ID, "AC-1550", 2)).thenReturn(Mono.empty());
        saveReturnsArgument();
        when(outbox.append(any(OrderEntity.class))).thenReturn(Mono.error(new IllegalStateException("outbox")));
        StepVerifier.create(service.confirm(ID)).expectErrorMessage("outbox").verify();
        verify(transactions).transactional(any(Mono.class));
    }

    @Test
    void rereadsAreLimitedToTwo() {
        when(repository.findById(ID)).thenAnswer(call -> Mono.just(order(OrderStatus.PENDING)));
        when(inventory.reserveStock(ID, "AC-1550", 2)).thenReturn(Mono.empty());
        when(repository.save(any(OrderEntity.class))).thenReturn(Mono.error(new OptimisticLockingFailureException("version")));

        StepVerifier.create(service.confirm(ID)).expectError(OptimisticLockingFailureException.class).verify();
        verify(repository, times(3)).findById(ID);
    }
}
