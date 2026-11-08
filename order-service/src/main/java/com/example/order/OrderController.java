package com.example.order;

import com.example.messaging.OrderCreated;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
class OrderController {
    private final ProductLookup products;
    private final OrderEventPublisher events;

    OrderController(ProductLookup products, OrderEventPublisher events) {
        this.products = products;
        this.events = events;
    }

    @PostMapping("/api/orders")
    @ResponseStatus(HttpStatus.CREATED)
    OrderCreated createBasic(@Valid @RequestBody BasicOrder request) {
        var event = new OrderCreated(UUID.randomUUID().toString(), request.customerEmail(),
                request.amount(), Instant.now(), null, null, "CREATED");
        events.publish(event, OrderEventPublisher.BASIC_TOPIC);
        return event;
    }

    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.CREATED)
    OrderResponse create(@Valid @RequestBody CatalogOrder request) {
        var product = products.findById(request.productId());
        var total = product.price().multiply(BigDecimal.valueOf(request.quantity()));
        var event = new OrderCreated(UUID.randomUUID().toString(), request.customerEmail(),
                total, Instant.now(), product.id(), request.quantity(), "CREATED");
        events.publish(event, OrderEventPublisher.CATALOG_TOPIC);
        return new OrderResponse(event.orderId(), event.status(), product, request.quantity(), total);
    }

    record BasicOrder(@NotBlank @Email String customerEmail, @NotNull @Positive BigDecimal amount) {}
    record CatalogOrder(@NotNull @Positive Long productId, @Positive int quantity,
                        @Email String customerEmail) {}
    record OrderResponse(String orderId, String status, ProductDto product, int quantity, BigDecimal total) {}
}
