package com.geovannycode.inventory.inventory.infrastructure.persistence;

import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ProductRepository extends ReactiveCrudRepository<ProductEntity, Long> {

    Mono<ProductEntity> findByCode(String code);

    Mono<Boolean> existsByCode(String code);

    Flux<ProductEntity> findAllByOrderByCodeAsc();

    /**
     * Descuenta stock mediante una única sentencia UPDATE condicional y atómica.
     * No hay ventana entre leer y escribir: MySQL serializa las modificaciones
     * de la fila, por lo que dos pedidos concurrentes no pueden dejar stock negativo.
     *
     * @param code código del producto
     * @param quantity cantidad positiva a descontar, validada por el llamador
     * @return 1 si se descontó; 0 si el producto no existe o el stock es insuficiente
     */
    @Modifying
    @Query("UPDATE products SET stock = stock - :quantity WHERE code = :code AND stock >= :quantity")
    Mono<Integer> decreaseStock(String code, int quantity);
}
