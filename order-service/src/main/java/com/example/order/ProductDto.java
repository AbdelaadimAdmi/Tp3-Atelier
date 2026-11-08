package com.example.order;

import java.math.BigDecimal;

public record ProductDto(long id, String name, BigDecimal price) {}
