package com.nantonijevic.feeds.integration;

import java.net.URI;

import com.nantonijevic.feeds.TestcontainersConfiguration;
import com.nantonijevic.feeds.dto.FeedResponse;
import com.nantonijevic.feeds.repository.FeedRepository;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@AutoConfigureWebTestClient
@Import(TestcontainersConfiguration.class)
class FeedApiIntegrationTest {

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private FeedRepository feedRepository;

    @BeforeEach
    void cleanDatabase() {
        StepVerifier.create(feedRepository.deleteAll())
                .verifyComplete();
    }

    @Test
    void postThenGetReturnsTheSameFeed() {
        EntityExchangeResult<FeedResponse> postResult =
                webTestClient.post()
                        .uri("/feeds")
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue("""
                                {
                                  "url": "https://example.com/rss.xml",
                                  "title": "Example"
                                }
                                """)
                        .exchange()
                        .expectStatus().isCreated()
                        .expectBody(FeedResponse.class)
                        .returnResult();

        FeedResponse created = postResult.getResponseBody();

        assertThat(created).isNotNull();
        assertThat(ObjectId.isValid(created.id())).isTrue();
        assertThat(created.createdAt()).isNotNull();
        assertThat(postResult.getResponseHeaders().getLocation())
                .isEqualTo(URI.create("/feeds/" + created.id()));

        webTestClient.get()
                .uri("/feeds/{id}", created.id())
                .exchange()
                .expectStatus().isOk()
                .expectBody(FeedResponse.class)
                .isEqualTo(created);
    }
}
