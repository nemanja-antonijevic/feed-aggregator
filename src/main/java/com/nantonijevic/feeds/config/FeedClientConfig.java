package com.nantonijevic.feeds.config;

import java.net.InetSocketAddress;
import java.time.Duration;

import com.nantonijevic.feeds.client.FeedSourceAddressResolverGroup;
import io.netty.resolver.AddressResolverGroup;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(FeedSourceProperties.class)
public class FeedClientConfig {

    private static final int MAX_BODY_SIZE = 1024 * 1024;

    @Bean
    WebClient feedWebClient(FeedSourceProperties properties) {
        HttpClient defaultClient = HttpClient.create();
        HttpClient httpClient = defaultClient
                .resolver(new FeedSourceAddressResolverGroup(
                        defaultAddressResolverGroup(defaultClient),
                        properties.isAllowPrivateAddresses()
                ))
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

    @SuppressWarnings("unchecked")
    private AddressResolverGroup<InetSocketAddress> defaultAddressResolverGroup(
            HttpClient httpClient
    ) {
        return (AddressResolverGroup<InetSocketAddress>) httpClient
                .configuration()
                .defaultAddressResolverGroup();
    }
}
