package com.geovannycode.order.order.infrastructure.persistence;

import java.util.List;

import com.geovannycode.order.order.domain.OrderStatus;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

/** Without these, Spring Data stores enum names ("PENDING"), which the status_order CHECK rejects. */
public final class OrderStatusConverters {

    private OrderStatusConverters() {
    }

    public static List<Converter<?, ?>> all() {
        return List.of(new OrderStatusWriter(), new OrderStatusReader());
    }

    @WritingConverter
    static final class OrderStatusWriter implements Converter<OrderStatus, String> {
        @Override
        public String convert(OrderStatus status) {
            return status.value();
        }
    }

    @ReadingConverter
    static final class OrderStatusReader implements Converter<String, OrderStatus> {
        @Override
        public OrderStatus convert(String value) {
            return OrderStatus.fromValue(value);
        }
    }
}
