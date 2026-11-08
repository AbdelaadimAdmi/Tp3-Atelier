package com.example.messaging;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class RabbitTopology {
    public static final String EXCHANGE = "orders.exchange";
    public static final String ROUTING_KEY = "order.created";
    public static final String QUEUE = "orders.notifications";
    public static final String DLX = "orders.dlx";
    public static final String DLQ = "orders.notifications.dlq";
    private static final String DEAD_KEY = "order.failed";

    @Bean
    Declarables orderTopology() {
        var exchange = new DirectExchange(EXCHANGE);
        var deadExchange = new DirectExchange(DLX);
        var queue = QueueBuilder.durable(QUEUE)
                .deadLetterExchange(DLX).deadLetterRoutingKey(DEAD_KEY).build();
        var deadQueue = QueueBuilder.durable(DLQ).build();
        return new Declarables(exchange, deadExchange, queue, deadQueue,
                BindingBuilder.bind(queue).to(exchange).with(ROUTING_KEY),
                BindingBuilder.bind(deadQueue).to(deadExchange).with(DEAD_KEY));
    }
}
