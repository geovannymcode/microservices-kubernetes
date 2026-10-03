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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
final class InventoryServiceTest {
    @Mock private ProductRepository repository;
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private InventoryService service;
    private static final String CODE = "UNIT-1";
    private static ProductEntity product(int stock) {
        return new ProductEntity(1L, CODE, "Lentes", new BigDecimal("123.50"), stock);
    }
    private static InventoryRequest request() {
        return new InventoryRequest(CODE, "Lentes", new BigDecimal("123.50"), 10);
    }
    @BeforeEach void createService() {
        service = new InventoryService(repository, meterRegistry);
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
        StepVerifier.create(service.decreaseStock(CODE, 5)).assertNext(p -> assertThat(p.stock()).isEqualTo(5)).verifyComplete();
        verify(repository, never()).existsByCode(anyString());
        assertThat(decreases("ok")).isEqualTo(1);
        assertThat(decreases("insufficient") + decreases("not_found")).isZero();
    }
    @Test void distinguishesMissingProductAfterFailedDecrement() {
        when(repository.decreaseStock(CODE, 5)).thenReturn(Mono.just(0));
        when(repository.existsByCode(CODE)).thenReturn(Mono.just(false));
        StepVerifier.create(service.decreaseStock(CODE, 5)).expectError(ProductNotFoundException.class).verify();
        verify(repository, never()).findByCode(anyString());
        assertThat(decreases("not_found")).isEqualTo(1);
        assertThat(decreases("ok") + decreases("insufficient")).isZero();
    }
    @Test void distinguishesInsufficientStockAfterFailedDecrement() {
        when(repository.decreaseStock(CODE, 5)).thenReturn(Mono.just(0));
        when(repository.existsByCode(CODE)).thenReturn(Mono.just(true));
        StepVerifier.create(service.decreaseStock(CODE, 5)).expectError(InsufficientStockException.class).verify();
        assertThat(decreases("insufficient")).isEqualTo(1);
        assertThat(decreases("ok") + decreases("not_found")).isZero();
    }
    @Test void listsProductsInRepositoryOrder() {
        when(repository.findAllByOrderByCodeAsc()).thenReturn(Flux.just(product(10)));
        StepVerifier.create(service.findAll()).assertNext(p -> assertThat(p.idProduct()).isEqualTo(CODE)).verifyComplete();
    }
}
