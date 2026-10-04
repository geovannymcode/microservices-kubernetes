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
import com.geovannycode.order.order.infrastructure.persistence.OrderEntity;
import com.geovannycode.order.order.infrastructure.persistence.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.OptimisticLockingFailureException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
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
    private OrderService service;

    @BeforeEach
    void createService() {
        service = new OrderService(repository, inventory, new OrderMapperImpl());
    }

    private static OrderEntity order(OrderStatus status) {
        return new OrderEntity(ID, "AC-1550", 2, status,
                status == OrderStatus.CANCELED ? CancelReason.INSUFFICIENT_STOCK : null, NOW, NOW, 0L);
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
    }

    @Test
    void missingOrderIsNotFoundNeverEmpty() {
        when(repository.findById(ID)).thenReturn(Mono.empty());
        StepVerifier.create(service.findById(ID)).expectError(OrderNotFoundException.class).verify();
        StepVerifier.create(service.confirm(ID)).expectError(OrderNotFoundException.class).verify();
        verifyNoInteractions(inventory);
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
        verifyNoInteractions(inventory);
        verify(repository, never()).save(any());
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
    }

    @Test
    void unavailableInventoryLeavesTheOrderPending() {
        when(repository.findById(ID)).thenReturn(Mono.just(order(OrderStatus.PENDING)));
        when(inventory.reserveStock(ID, "AC-1550", 2))
                .thenReturn(Mono.error(new InventoryUnavailableException(new RuntimeException("down"))));
        StepVerifier.create(service.confirm(ID)).expectError(InventoryUnavailableException.class).verify();
        verify(repository, never()).save(any());
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
