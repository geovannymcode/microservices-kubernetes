package com.geovannycode.inventory.inventory.infrastructure.persistence;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import com.geovannycode.inventory.TestcontainersConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@DataR2dbcTest
@Import(TestcontainersConfiguration.class)
@ImportAutoConfiguration(FlywayAutoConfiguration.class)
final class ProductRepositoryIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private final ProductRepository repository;

    @Autowired
    ProductRepositoryIT(ProductRepository repository) {
        this.repository = repository;
    }

    @Test
    void findByCodeReturnsSeedProduct() {
        StepVerifier.create(repository.findByCode("AC-1550"))
                .assertNext(product -> {
                    assertThat(product.id()).isNotNull();
                    assertThat(product.code()).isEqualTo("AC-1550");
                    assertThat(product.nameProduct()).isEqualTo("Lentes");
                    assertThat(product.price()).isEqualByComparingTo("123.50");
                    assertThat(product.stock()).isEqualTo(50);
                })
                .expectComplete().verify(TIMEOUT);
    }

    @Test
    void saveRejectsDuplicateCode() {
        var duplicate = new ProductEntity(null, "AC-1550", "Duplicado", new BigDecimal("10.00"), 1);
        StepVerifier.create(repository.save(duplicate))
                .expectError(DataIntegrityViolationException.class).verify(TIMEOUT);
    }

    @Test
    void databaseRejectsLowercaseCodesThatBypassTheApi() {
        var lowercase = new ProductEntity(null, "it-" + UUID.randomUUID(), "Minúsculas", new BigDecimal("10.00"), 1);
        // r2dbc-mysql surfaces error 3819 as a resource failure, not an integrity violation; the handler maps it to 500.
        StepVerifier.create(repository.save(lowercase))
                .expectErrorSatisfies(error -> assertThat(error).hasMessageContaining("ck_products_code_format"))
                .verify(TIMEOUT);
    }

    @Test
    void decreaseStockUpdatesProductWhenStockIsSufficient() {
        StepVerifier.create(createProduct(10).flatMap(product ->
                repository.decreaseStock(product.code(), 4)
                        .doOnNext(rows -> assertThat(rows).isEqualTo(1))
                        .then(repository.findByCode(product.code()))))
                .assertNext(product -> assertThat(product.stock()).isEqualTo(6))
                .expectComplete().verify(TIMEOUT);
    }

    @Test
    void decreaseStockLeavesProductUnchangedWhenStockIsInsufficient() {
        StepVerifier.create(createProduct(3).flatMap(product ->
                repository.decreaseStock(product.code(), 4)
                        .doOnNext(rows -> assertThat(rows).isZero())
                        .then(repository.findByCode(product.code()))))
                .assertNext(product -> assertThat(product.stock()).isEqualTo(3))
                .expectComplete().verify(TIMEOUT);
    }

    @Test
    void decreaseStockReturnsZeroForMissingProduct() {
        StepVerifier.create(repository.decreaseStock("MISSING-" + UUID.randomUUID().toString().toUpperCase(), 1))
                .expectNext(0).expectComplete().verify(TIMEOUT);
    }

    @Test
    void concurrentDecreasesCannotOversell() {
        StepVerifier.create(createProduct(10).flatMap(product ->
                Flux.range(0, 20)
                        .flatMap(ignored -> repository.decreaseStock(product.code(), 1), 20)
                        .collectList()
                        .doOnNext(results -> {
                            assertThat(results).hasSize(20).containsOnly(0, 1);
                            assertThat(results.stream().mapToInt(Integer::intValue).sum()).isEqualTo(10);
                        })
                        .then(repository.findByCode(product.code()))))
                .assertNext(product -> assertThat(product.stock()).isZero())
                .expectComplete().verify(TIMEOUT);
    }

    private Mono<ProductEntity> createProduct(int stock) {
        // Cada caso modifica su propia fila; el contenedor se descarta al finalizar.
        return repository.save(new ProductEntity(null, "IT-" + UUID.randomUUID().toString().toUpperCase(),
                "Producto de prueba", new BigDecimal("10.00"), stock));
    }
}
