package com.geovannycode.order.order.infrastructure.persistence;

import com.geovannycode.order.order.domain.OrderStatus;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface OrderRepository extends ReactiveCrudRepository<OrderEntity, Long> {

    Flux<OrderEntity> findAllByOrderByIdAsc();

    Flux<OrderEntity> findAllByStatusOrderByIdAsc(OrderStatus status);
}
