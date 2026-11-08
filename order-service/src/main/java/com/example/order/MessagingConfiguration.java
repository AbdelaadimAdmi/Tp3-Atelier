package com.example.order;

import com.example.messaging.RabbitTopology;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration(proxyBeanMethods = false)
@Import(RabbitTopology.class)
class MessagingConfiguration {
    @Bean
    KafkaAdmin.NewTopics orderTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(OrderEventPublisher.BASIC_TOPIC).partitions(3).replicas(1).build(),
                TopicBuilder.name(OrderEventPublisher.CATALOG_TOPIC).partitions(3).replicas(1).build(),
                TopicBuilder.name(OrderEventPublisher.BASIC_TOPIC + ".DLT").partitions(3).replicas(1).build(),
                TopicBuilder.name(OrderEventPublisher.CATALOG_TOPIC + ".DLT").partitions(3).replicas(1).build());
    }
}
