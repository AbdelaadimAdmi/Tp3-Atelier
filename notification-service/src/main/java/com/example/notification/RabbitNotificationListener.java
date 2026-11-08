package com.example.notification;

import com.example.messaging.OrderCreated;
import com.example.messaging.RabbitTopology;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Component
@Import(RabbitTopology.class)
class RabbitNotificationListener {
    private static final Logger log = LoggerFactory.getLogger(RabbitNotificationListener.class);
    private final ObjectMapper json;
    private final Path deliveries;

    RabbitNotificationListener(ObjectMapper json, @Value("${atelier.notification.directory}") Path deliveries) {
        this.json = json;
        this.deliveries = deliveries;
    }

    @RabbitListener(queues = RabbitTopology.QUEUE)
    void receive(Message message) throws IOException, InterruptedException {
        var event = json.readValue(message.getBody(), OrderCreated.class);
        var id = UUID.fromString(event.orderId());
        if (event.amount() == null || event.amount().signum() <= 0) {
            throw new IllegalArgumentException("Montant invalide pour " + id);
        }
        log.info("RabbitMQ reçu orderId={} redelivered={}", id, message.getMessageProperties().isRedelivered());
        if (event.failsDemo()) {
            log.warn("Échec RabbitMQ simulé orderId={}", id);
            throw new IllegalStateException("Échec de notification simulé");
        }
        if (event.slowsDemo()) {
            log.info("Traitement lent RabbitMQ orderId={}", id);
            Thread.sleep(10_000);
        }

        Files.createDirectories(deliveries);
        try {
            // The creation of this unique marker is the simulated notification.
            Files.createFile(deliveries.resolve(id + ".sent"));
        } catch (FileAlreadyExistsException duplicate) {
            log.info("Notification déjà traitée orderId={}", id);
            return;
        }
        log.info("Notification simulée orderId={} email={}", id, event.customerEmail());
    }
}
