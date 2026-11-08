package com.example.order;

import com.example.messaging.OrderCreated;
import com.example.messaging.RabbitTopology;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
class OrderEventPublisher {
    static final String BASIC_TOPIC = "commandes-creees";
    static final String CATALOG_TOPIC = "orders-events";
    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);
    private final ObjectMapper json;
    private final RabbitTemplate rabbit;
    private final KafkaTemplate<String, String> kafka;

    OrderEventPublisher(ObjectMapper json, RabbitTemplate rabbit, KafkaTemplate<String, String> kafka) {
        this.json = json;
        this.rabbit = rabbit;
        this.kafka = kafka;
    }

    void publish(OrderCreated event, String topic) {
        String payload;
        try {
            payload = json.writeValueAsString(event);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Sérialisation de la commande impossible", error);
        }
        sendRabbit(event.orderId(), payload);
        try {
            kafka.send(topic, event.orderId(), payload).whenComplete((result, error) -> {
                if (error != null) {
                    log.error("Échec Kafka orderId={}", event.orderId(), error);
                } else {
                    var m = result.getRecordMetadata();
                    log.info("Kafka publié key={} topic={} partition={} offset={}",
                            event.orderId(), m.topic(), m.partition(), m.offset());
                }
            });
        } catch (RuntimeException error) {
            log.error("Échec Kafka orderId={}", event.orderId(), error);
        }
    }

    private void sendRabbit(String orderId, String payload) {
        var message = MessageBuilder.withBody(payload.getBytes(StandardCharsets.UTF_8))
                .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .setMessageId(orderId)
                .build();
        var correlation = new CorrelationData(orderId);
        try {
            rabbit.send(RabbitTopology.EXCHANGE, RabbitTopology.ROUTING_KEY, message, correlation);
            var confirm = correlation.getFuture().get(3, TimeUnit.SECONDS);
            if (!confirm.isAck() || correlation.getReturned() != null) {
                throw new AmqpException("Publication RabbitMQ rejetée ou sans destination");
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw rabbitUnavailable(error);
        } catch (AmqpException | ExecutionException | TimeoutException error) {
            throw rabbitUnavailable(error);
        }
    }

    private ResponseStatusException rabbitUnavailable(Exception error) {
        log.error("Publication RabbitMQ non confirmée", error);
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                "Publication de la commande non confirmée.", error);
    }
}
