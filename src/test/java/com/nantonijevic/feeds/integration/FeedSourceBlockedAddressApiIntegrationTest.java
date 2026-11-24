package com.nantonijevic.feeds.integration;

import java.io.IOException;
import java.time.Instant;

import com.nantonijevic.feeds.TestcontainersConfiguration;
import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.repository.FeedRepository;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@AutoConfigureWebTestClient
@Import(TestcontainersConfiguration.class)
class FeedSourceBlockedAddressApiIntegrationTest {

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private FeedRepository feedRepository;

    @BeforeEach
    void cleanDatabase() {
        StepVerifier.create(feedRepository.deleteAll()).verifyComplete();
    }

    @Test
    void fetchOfALoopbackFeedReturns422AndNeverContactsTheSource()
            throws IOException {
        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(new MockResponse.Builder().code(200).body("x").build());
            Feed feed = feedRepository.save(new Feed(
                    null,
                    source.url("/rss.xml").toString(),
                    "Loopback",
                    Instant.parse("2026-11-24T08:00:00Z")
            )).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus().isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT)
                    .expectBody()
                    .jsonPath("$.message")
                    .isEqualTo("feed source address is not allowed");

            assertThat(source.getRequestCount()).isZero();
        }
    }
}
