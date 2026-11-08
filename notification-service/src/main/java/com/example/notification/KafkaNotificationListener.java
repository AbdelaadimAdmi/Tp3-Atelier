package com.example.notification;

import com.example.messaging.OrderCreated;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
class KafkaNotificationListener {
    private static final Logger log = LoggerFactory.getLogger(KafkaNotificationListener.class);
    private final ObjectMapper json;

    KafkaNotificationListener(ObjectMapper json) {
        this.json = json;
    }

    @KafkaListener(topics = {"commandes-creees", "orders-events"})
    void receive(ConsumerRecord<String, String> record, Acknowledgment acknowledgment)
            throws JsonProcessingException {
        var event = json.readValue(record.value(), OrderCreated.class);
        if (event.failsDemo()) {
            log.warn("Échec Kafka simulé key={} partition={} offset={}",
                    record.key(), record.partition(), record.offset());
            throw new IllegalStateException("Échec Kafka simulé");
        }
        log.info("Kafka reçu topic={} key={} partition={} offset={}",
                record.topic(), record.key(), record.partition(), record.offset());
        acknowledgment.acknowledge();
    }
}
