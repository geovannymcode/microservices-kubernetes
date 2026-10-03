package com.geovannycode.inventory.inventory.application;

import java.math.BigDecimal;
import com.geovannycode.inventory.inventory.api.dto.InventoryRequest;
import com.geovannycode.inventory.inventory.domain.*;
import com.geovannycode.inventory.inventory.infrastructure.persistence.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
final class InventoryServiceTest {
    @Mock private ProductRepository repository;
    @Mock private StockMovementRepository movements;
    @Mock private ReactiveTransactionManager transactionManager;
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private InventoryService service;
    private static final String CODE = "UNIT-1";
    private static final String KEY = "order-1";
    private static ProductEntity product(int stock) {
        return new ProductEntity(1L, CODE, "Lentes", new BigDecimal("123.50"), stock);
    }
    private static InventoryRequest request() {
        return new InventoryRequest(CODE, "Lentes", new BigDecimal("123.50"), 10);
    }
    @BeforeEach void createService() {
        lenient().when(transactionManager.getReactiveTransaction(any())).thenReturn(Mono.just(mock(ReactiveTransaction.class)));
        lenient().when(transactionManager.commit(any())).thenReturn(Mono.empty());
        lenient().when(transactionManager.rollback(any())).thenReturn(Mono.empty());
        service = new InventoryService(repository, movements, TransactionalOperator.create(transactionManager), meterRegistry);
    }
    private double decreases(String result) {
        return meterRegistry.get("inventory.stock.decrease").tag("result", result).counter().count();
    }
    private double registered() {
        return meterRegistry.get("inventory.product.registered").counter().count();
    }
    @Test void registersProductUsingUniqueConstraint() {
        when(repository.save(any(ProductEntity.class))).thenReturn(Mono.just(product(10)));
        StepVerifier.create(service.register(request())).assertNext(response -> {
            assertThat(response.idProduct()).isEqualTo(CODE);
            assertThat(response.stock()).isEqualTo(10);
        }).verifyComplete();
        verify(repository).save(new ProductEntity(null, CODE, "Lentes", new BigDecimal("123.50"), 10));
        verify(repository, never()).existsByCode(anyString());
        assertThat(registered()).isEqualTo(1);
    }
    @Test void translatesDuplicateKey() {
        when(repository.save(any(ProductEntity.class))).thenReturn(Mono.error(new DuplicateKeyException("duplicate")));
        StepVerifier.create(service.register(request())).expectError(DuplicateProductException.class).verify();
        assertThat(registered()).isZero();
    }
    @Test void findsExistingProduct() {
        when(repository.findByCode(CODE)).thenReturn(Mono.just(product(10)));
        StepVerifier.create(service.findByCode(CODE)).assertNext(p -> assertThat(p.idProduct()).isEqualTo(CODE)).verifyComplete();
    }
    @Test void reportsMissingProduct() {
        when(repository.findByCode(CODE)).thenReturn(Mono.empty());
        StepVerifier.create(service.findByCode(CODE)).expectError(ProductNotFoundException.class).verify();
    }
    @Test void returnsUpdatedStockAfterSuccessfulDecrement() {
        when(repository.decreaseStock(CODE, 5)).thenReturn(Mono.just(1));
        when(repository.findByCode(CODE)).thenReturn(Mono.just(product(5)));
        StepVerifier.create(service.decreaseStock(CODE, 5, null)).assertNext(p -> assertThat(p.stock()).isEqualTo(5)).verifyComplete();
        verify(repository, never()).existsByCode(anyString());
        assertThat(decreases("ok")).isEqualTo(1);
        assertThat(decreases("insufficient") + decreases("not_found")).isZero();
    }
    @Test void distinguishesMissingProductAfterFailedDecrement() {
        when(repository.decreaseStock(CODE, 5)).thenReturn(Mono.just(0));
        when(repository.existsByCode(CODE)).thenReturn(Mono.just(false));
        StepVerifier.create(service.decreaseStock(CODE, 5, null)).expectError(ProductNotFoundException.class).verify();
        verify(repository, never()).findByCode(anyString());
        assertThat(decreases("not_found")).isEqualTo(1);
        assertThat(decreases("ok") + decreases("insufficient")).isZero();
    }
    @Test void distinguishesInsufficientStockAfterFailedDecrement() {
        when(repository.decreaseStock(CODE, 5)).thenReturn(Mono.just(0));
        when(repository.existsByCode(CODE)).thenReturn(Mono.just(true));
        StepVerifier.create(service.decreaseStock(CODE, 5, null)).expectError(InsufficientStockException.class).verify();
        assertThat(decreases("insufficient")).isEqualTo(1);
        assertThat(decreases("ok") + decreases("not_found")).isZero();
    }
    @Test void decreasesWithoutIdempotencyKeyNeverTouchesMovements() {
        when(repository.decreaseStock(CODE, 5)).thenReturn(Mono.just(1));
        when(repository.findByCode(CODE)).thenReturn(Mono.just(product(5)));
        StepVerifier.create(service.decreaseStock(CODE, 5, null)).expectNextCount(1).verifyComplete();
        verifyNoInteractions(movements);
    }
    @Test void recordsKeyAndDecreasesOnFirstAttempt() {
        when(movements.insert(KEY, CODE, 2)).thenReturn(Mono.just(1));
        when(repository.decreaseStock(CODE, 2)).thenReturn(Mono.just(1));
        when(repository.findByCode(CODE)).thenReturn(Mono.just(product(8)));
        StepVerifier.create(service.decreaseStock(CODE, 2, KEY)).assertNext(p -> assertThat(p.stock()).isEqualTo(8)).verifyComplete();
        assertThat(decreases("ok")).isEqualTo(1);
        assertThat(decreases("replayed")).isZero();
    }
    @Test void replaysSameKeyAndRequestWithoutDecreasingAgain() {
        when(movements.insert(KEY, CODE, 2)).thenReturn(Mono.error(new DuplicateKeyException("duplicate")));
        when(movements.findById(KEY)).thenReturn(Mono.just(new StockMovementEntity(KEY, CODE, 2)));
        when(repository.findByCode(CODE)).thenReturn(Mono.just(product(8)));
        StepVerifier.create(service.decreaseStock(CODE, 2, KEY)).assertNext(p -> assertThat(p.stock()).isEqualTo(8)).verifyComplete();
        verify(repository, never()).decreaseStock(anyString(), anyInt());
        assertThat(decreases("replayed")).isEqualTo(1);
        assertThat(decreases("ok")).isZero();
    }
    @Test void rejectsKeyReusedForDifferentQuantity() {
        when(movements.insert(KEY, CODE, 3)).thenReturn(Mono.error(new DuplicateKeyException("duplicate")));
        when(movements.findById(KEY)).thenReturn(Mono.just(new StockMovementEntity(KEY, CODE, 2)));
        StepVerifier.create(service.decreaseStock(CODE, 3, KEY)).expectError(IdempotencyKeyReusedException.class).verify();
        verify(repository, never()).decreaseStock(anyString(), anyInt());
        verify(repository, never()).findByCode(anyString());
    }
    @Test void rejectsKeyReusedForDifferentProduct() {
        when(movements.insert(KEY, CODE, 2)).thenReturn(Mono.error(new DuplicateKeyException("duplicate")));
        when(movements.findById(KEY)).thenReturn(Mono.just(new StockMovementEntity(KEY, "OTHER-1", 2)));
        StepVerifier.create(service.decreaseStock(CODE, 2, KEY)).expectError(IdempotencyKeyReusedException.class).verify();
    }
    @Test void propagatesInsufficientStockAfterRecordingKeySoTransactionRollsItBack() {
        when(movements.insert(KEY, CODE, 2)).thenReturn(Mono.just(1));
        when(repository.decreaseStock(CODE, 2)).thenReturn(Mono.just(0));
        when(repository.existsByCode(CODE)).thenReturn(Mono.just(true));
        StepVerifier.create(service.decreaseStock(CODE, 2, KEY)).expectError(InsufficientStockException.class).verify();
        assertThat(decreases("insufficient")).isEqualTo(1);
    }
    @Test void rejectsNonPositiveQuantityWithoutTouchingTheDatabase() {
        StepVerifier.create(service.decreaseStock(CODE, 0, null)).expectError(IllegalArgumentException.class).verify();
        StepVerifier.create(service.decreaseStock(CODE, -5, KEY)).expectError(IllegalArgumentException.class).verify();
        verifyNoInteractions(repository, movements);
    }
    @Test void reportsOtherIntegrityViolationsInsteadOfDuplicate() {
        when(repository.save(any(ProductEntity.class)))
                .thenReturn(Mono.error(new org.springframework.dao.DataIntegrityViolationException("ck_products_price")));
        StepVerifier.create(service.register(request()))
                .expectErrorMatches(error -> !(error instanceof DuplicateProductException)).verify();
    }
    @Test void failedCommitIsNotCountedAsSuccessfulDecrease() {
        when(transactionManager.commit(any())).thenReturn(Mono.error(new TransactionSystemException("commit failed")));
        when(repository.decreaseStock(CODE, 1)).thenReturn(Mono.just(1));
        when(repository.findByCode(CODE)).thenReturn(Mono.just(product(9)));
        StepVerifier.create(service.decreaseStock(CODE, 1, null)).expectError(TransactionSystemException.class).verify();
        assertThat(decreases("ok")).isZero();
    }
    @Test void insufficientStockRollsBackInsteadOfCommitting() {
        when(repository.decreaseStock(CODE, 1)).thenReturn(Mono.just(0));
        when(repository.existsByCode(CODE)).thenReturn(Mono.just(true));
        StepVerifier.create(service.decreaseStock(CODE, 1, null)).expectError(InsufficientStockException.class).verify();
        verify(transactionManager).rollback(any());
        verify(transactionManager, never()).commit(any());
    }
    @Test void listsProductsInRepositoryOrder() {
        when(repository.findAllByOrderByCodeAsc(org.springframework.data.domain.PageRequest.of(1, 5)))
                .thenReturn(Flux.just(product(10)));
        StepVerifier.create(service.findAll(1, 5)).assertNext(p -> assertThat(p.idProduct()).isEqualTo(CODE)).verifyComplete();
    }
}
