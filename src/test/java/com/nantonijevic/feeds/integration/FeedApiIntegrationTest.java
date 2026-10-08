package com.nantonijevic.feeds.integration;

import java.net.URI;
import java.time.Instant;
import java.util.List;

import com.nantonijevic.feeds.TestcontainersConfiguration;
import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.dto.FeedResponse;
import com.nantonijevic.feeds.repository.FeedRepository;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
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

    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;

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

    @Test
    void getFeedsReturnsEmptyListWhenNoFeedsExist() {
        webTestClient.get()
                .uri("/feeds")
                .exchange()
                .expectStatus().isOk()
                .expectHeader()
                .contentType(MediaType.APPLICATION_JSON)
                .expectBody()
                .json("[]");
    }

    @Test
    void getFeedsReturnsNewerFeedsFirst() {
        Feed older = new Feed(
                null,
                "https://example.com/older.xml",
                "Older",
                Instant.parse("2026-10-08T08:00:00Z")
        );
        Feed newer = new Feed(
                null,
                "https://example.com/newer.xml",
                "Newer",
                Instant.parse("2026-10-08T09:00:00Z")
        );

        StepVerifier.create(
                        feedRepository.saveAll(List.of(older, newer))
                )
                .expectNextCount(2)
                .verifyComplete();

        webTestClient.get()
                .uri("/feeds")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].title").isEqualTo("Newer")
                .jsonPath("$[1].title").isEqualTo("Older");
    }

    @Test
    void getFeedsUsesIdDescendingAsTieBreaker() {
        Instant createdAt = Instant.parse("2026-10-08T09:00:00Z");

        Feed lowerId = new Feed(
                "507f1f77bcf86cd799439011",
                "https://example.com/lower-id.xml",
                "Lower ID",
                createdAt
        );
        Feed higherId = new Feed(
                "507f1f77bcf86cd799439012",
                "https://example.com/higher-id.xml",
                "Higher ID",
                createdAt
        );

        StepVerifier.create(
                        feedRepository.saveAll(List.of(lowerId, higherId))
                )
                .expectNextCount(2)
                .verifyComplete();

        webTestClient.get()
                .uri("/feeds")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].id").isEqualTo(higherId.getId())
                .jsonPath("$[1].id").isEqualTo(lowerId.getId());
    }

    @Test
    void deleteExistingFeedReturnsNoContentAndFeedIsGone() {
        EntityExchangeResult<FeedResponse> postResult =
                webTestClient.post()
                        .uri("/feeds")
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue("""
                            {
                              "url": "https://example.com/delete-me.xml",
                              "title": "Delete me"
                            }
                            """)
                        .exchange()
                        .expectStatus().isCreated()
                        .expectBody(FeedResponse.class)
                        .returnResult();

        FeedResponse created = postResult.getResponseBody();
        assertThat(created).isNotNull();

        webTestClient.delete()
                .uri("/feeds/{id}", created.id())
                .exchange()
                .expectStatus().isNoContent()
                .expectBody()
                .isEmpty();

        webTestClient.get()
                .uri("/feeds/{id}", created.id())
                .exchange()
                .expectStatus().isNotFound();

        webTestClient.delete()
                .uri("/feeds/{id}", created.id())
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo("feed not found: " + created.id());
    }

    @Test
    void deleteMissingFeedReturnsNotFound() {
        String id = "507f1f77bcf86cd799439099";

        webTestClient.delete()
                .uri("/feeds/{id}", id)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo("feed not found: " + id);
    }

    @Test
    void deleteMalformedObjectIdReturnsBadRequest() {
        webTestClient.delete()
                .uri("/feeds/abc")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo("id must be a valid ObjectId: abc");
    }

    @Test
    void createsUniqueIndexForFeedUrl() {
        StepVerifier.create(
                        reactiveMongoTemplate
                                .indexOps(Feed.class)
                                .getIndexInfo()
                                .filter(index ->
                                        "url_unique".equals(index.getName())
                                )
                                .single()
                )
                .assertNext(index -> {
                    assertThat(index.isUnique()).isTrue();
                    assertThat(index.getIndexFields())
                            .extracting(IndexField::getKey)
                            .containsExactly("url");
                })
                .verifyComplete();
    }

    @Test
    void duplicateUrlReturnsConflict() {
        String requestBody = """
            {
              "url": "https://example.com/duplicate.xml",
              "title": "Example"
            }
            """;

        webTestClient.post()
                .uri("/feeds")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(requestBody)
                .exchange()
                .expectStatus().isCreated();

        webTestClient.post()
                .uri("/feeds")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(requestBody)
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.CONFLICT)
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo("url already exists");
    }

    @Test
    void concurrentPostsWithSameUrlCreateExactlyOneFeed() {
        String requestBody = """
                {
                  "url": "https://example.com/concurrent-duplicate.xml",
                  "title": "Concurrent example"
                }
                """;

        List<Integer> statuses = Flux.range(0, 10)
                .flatMap(ignored ->
                                Mono.fromCallable(() ->
                                                postFeed(requestBody)
                                        )
                                        .subscribeOn(Schedulers.boundedElastic()),
                        10
                )
                .collectList()
                .block();

        assertThat(statuses).isNotNull();
        assertThat(statuses).hasSize(10);
        assertThat(statuses)
                .filteredOn(status -> status == 201)
                .hasSize(1);
        assertThat(statuses)
                .filteredOn(status -> status == 409)
                .hasSize(9);

        StepVerifier.create(feedRepository.count())
                .expectNext(1L)
                .verifyComplete();
    }

    private int postFeed(String requestBody) {
        return webTestClient.post()
                .uri("/feeds")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(requestBody)
                .exchange()
                .expectBody()
                .returnResult()
                .getStatus()
                .value();
    }
}
