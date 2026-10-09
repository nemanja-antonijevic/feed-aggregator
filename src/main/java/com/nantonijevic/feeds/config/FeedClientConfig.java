package com.nantonijevic.feeds.config;

import java.time.Duration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

@Configuration(proxyBeanMethods = false)
public class FeedClientConfig {

    private static final int MAX_BODY_SIZE = 1024 * 1024;

    @Bean
    WebClient feedWebClient() {
        HttpClient httpClient = HttpClient.create()
                .responseTimeout(Duration.ofSeconds(3));

        return WebClient.builder()
                .clientConnector(
                        new ReactorClientHttpConnector(httpClient)
                )
                .codecs(configurer ->
                        configurer
                                .defaultCodecs()
                                .maxInMemorySize(MAX_BODY_SIZE)
                )
                .build();
    }
}
