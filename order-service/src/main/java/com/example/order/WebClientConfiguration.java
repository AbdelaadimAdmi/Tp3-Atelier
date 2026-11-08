package com.example.order;

import io.netty.channel.ChannelOption;
import java.time.Duration;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

@Configuration(proxyBeanMethods = false)
class WebClientConfiguration {
    @Bean
    @LoadBalanced
    WebClient.Builder productWebClient() {
        var http = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 700)
                .responseTimeout(Duration.ofMillis(900));
        return WebClient.builder().clientConnector(new ReactorClientHttpConnector(http));
    }
}
