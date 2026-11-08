package com.example.messaging;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreated(String orderId, String customerEmail, BigDecimal amount,
                           Instant createdAt, Long productId, Integer quantity, String status) {
    public boolean failsDemo() {
        return quantity != null ? quantity == 13 : amount.compareTo(BigDecimal.valueOf(13)) == 0;
    }

    public boolean slowsDemo() {
        return quantity != null ? quantity == 7 : amount.compareTo(BigDecimal.valueOf(7)) == 0;
    }
}
