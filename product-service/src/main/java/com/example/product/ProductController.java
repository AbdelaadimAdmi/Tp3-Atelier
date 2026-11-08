package com.example.product;

import java.math.BigDecimal;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
class ProductController {
    private static final ProductDto KEYBOARD = new ProductDto(1L, "Clavier", new BigDecimal("249.90"));
    private final String welcome;

    ProductController(@Value("${message.bienvenue}") String welcome) {
        this.welcome = welcome;
    }

    @GetMapping("/products/{id}")
    ProductDto findById(@PathVariable long id) {
        if (id != KEYBOARD.id()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Produit introuvable");
        }
        return KEYBOARD;
    }

    @GetMapping("/products/config")
    Map<String, String> configuration() {
        return Map.of("bienvenue", welcome);
    }

    record ProductDto(long id, String name, BigDecimal price) {}
}
