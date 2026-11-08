package com.example.order;

import feign.FeignException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import java.time.Duration;
import java.util.function.LongFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ResponseStatusException;

@Service
class ProductLookup {
    private static final Logger log = LoggerFactory.getLogger(ProductLookup.class);
    private final CircuitBreaker breaker;
    private final Retry retry;
    private final LongFunction<ProductDto> client;

    ProductLookup(ProductClient feign, WebClient.Builder builder, CircuitBreakerRegistry breakers,
                  RetryRegistry retries, @Value("${atelier.product.client:FEIGN}") Client selected) {
        breaker = breakers.circuitBreaker("productService");
        retry = retries.retry("productService");
        var webClient = builder.baseUrl("http://PRODUCT-SERVICE").build();
        client = switch (selected) {
            case FEIGN -> feign::findById;
            case WEBCLIENT -> id -> webClient.get().uri("/products/{id}", id)
                    .retrieve().bodyToMono(ProductDto.class).block(Duration.ofMillis(1100));
        };
        log.info("Client produit actif : {}", selected);
        retry.getEventPublisher().onRetry(event ->
                log.warn("Nouvelle tentative Product Service ({})", event.getNumberOfRetryAttempts()));
        breaker.getEventPublisher().onStateTransition(event ->
                log.info("Circuit productService : {}", event.getStateTransition()));
    }

    ProductDto findById(long id) {
        try {
            return breaker.executeSupplier(() -> retry.executeSupplier(() -> fetch(id)));
        } catch (CallNotPermittedException | Unavailable error) {
            log.warn("Fallback Product Service : {}", error.getClass().getSimpleName());
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Catalogue indisponible. Commande non créée.", error);
        }
    }

    private ProductDto fetch(long id) {
        try {
            var product = client.apply(id);
            if (product == null || product.price() == null) {
                throw new IllegalStateException("Réponse produit incomplète");
            }
            return product;
        } catch (FeignException.NotFound | WebClientResponseException.NotFound error) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Produit introuvable", error);
        } catch (RuntimeException error) {
            throw new Unavailable(error);
        }
    }

    enum Client { FEIGN, WEBCLIENT }

    public static class Unavailable extends RuntimeException {
        Unavailable(Throwable cause) {
            super(cause);
        }
    }
}

