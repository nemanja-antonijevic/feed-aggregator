package com.nantonijevic.feeds.client;

import java.io.IOException;
import java.time.Duration;

import com.nantonijevic.feeds.config.FeedClientConfig;
import com.nantonijevic.feeds.exception.InvalidFeedSourceException;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class FeedSourceClientBlockedAddressIntegrationTest {

    @Test
    void loopbackSourceIsRefusedBeforeAnyRequestWhenPrivateAddressesAreOff()
            throws IOException {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        context.register(FeedClientConfig.class);
        context.refresh();

        try (context; MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(new MockResponse.Builder().code(200).body("x").build());
            FeedSourceClient client = new FeedSourceClient(
                    context.getBean(WebClient.class)
            );

            StepVerifier.create(client.fetch(source.url("/feed.xml").toString()))
                    .expectErrorSatisfies(error ->
                            assertThat(error)
                                    .isInstanceOf(InvalidFeedSourceException.class)
                                    .hasMessage("feed source address is not allowed")
                    )
                    .verify(Duration.ofSeconds(10));

            assertThat(source.getRequestCount()).isZero();
        }
    }
}
