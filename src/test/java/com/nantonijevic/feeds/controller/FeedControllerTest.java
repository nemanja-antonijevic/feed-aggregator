package com.nantonijevic.feeds.controller;

import java.time.Instant;
import java.util.Map;

import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.domain.PollOutcome;
import com.nantonijevic.feeds.domain.PollStatus;
import com.nantonijevic.feeds.exception.FeedNotFoundException;
import com.nantonijevic.feeds.exception.InvalidFeedIdException;
import com.nantonijevic.feeds.exception.ItemPersistenceException;
import com.nantonijevic.feeds.service.FeedService;
import com.nantonijevic.feeds.service.FetchFeedService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@WebFluxTest(FeedController.class)
class FeedControllerTest {

    private static final String ID =
            "507f1f77bcf86cd799439011";

    private static final Instant CREATED_AT =
            Instant.parse("2026-10-07T07:00:00Z");

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private FeedService feedService;

    @MockitoBean
    private FetchFeedService fetchFeedService;

    @Test
    void createsFeed() {
        Feed feed = new Feed(
                ID,
                "https://example.com/rss.xml",
                "Example",
                CREATED_AT
        );

        when(feedService.create(
                "https://example.com/rss.xml",
                "Example"
        )).thenReturn(Mono.just(feed));

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
                .expectHeader().valueEquals(
                        "Location",
                        "/feeds/" + ID
                )
                .expectBody()
                .jsonPath("$.id").isEqualTo(ID)
                .jsonPath("$.url")
                .isEqualTo("https://example.com/rss.xml")
                .jsonPath("$.title").isEqualTo("Example")
                .jsonPath("$.createdAt")
                .isEqualTo("2026-10-07T07:00:00Z")
                .jsonPath("$").value(document -> {
                    Map<?, ?> response = (Map<?, ?>) document;
                    assertThat(response.containsKey("pollStatus")).isTrue();
                    assertThat(response.get("pollStatus")).isNull();
                });
    }

    @Test
    void rejectsMissingUrl() {
        assertBadRequest(
                """
                {"title": "Example"}
                """,
                "url must not be blank"
        );
    }

    @Test
    void rejectsBlankUrl() {
        assertBadRequest(
                """
                {"url": " ", "title": "Example"}
                """,
                "url must not be blank"
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "example.com/rss.xml",
            "ftp://example.com/rss.xml"
    })
    void rejectsUrlThatIsNotAbsoluteHttpOrHttps(String url) {
        assertBadRequest(
                """
                {"url": "%s", "title": "Example"}
                """.formatted(url),
                "url must be an absolute http or https URL"
        );
    }

    @Test
    void rejectsMissingTitle() {
        assertBadRequest(
                """
                {"url": "https://example.com/rss.xml"}
                """,
                "title must not be blank"
        );
    }

    @Test
    void rejectsBlankTitle() {
        assertBadRequest(
                """
                {
                  "url": "https://example.com/rss.xml",
                  "title": " "
                }
                """,
                "title must not be blank"
        );
    }

    @Test
    void rejectsMissingUrlAndTitleDeterministically() {
        assertBadRequest(
                "{}",
                "title must not be blank"
        );
    }

    @Test
    void returnsExistingFeed() {
        Feed feed = new Feed(
                ID,
                "https://example.com/rss.xml",
                "Example",
                CREATED_AT
        );

        when(feedService.findById(ID))
                .thenReturn(Mono.just(feed));

        webTestClient.get()
                .uri("/feeds/{id}", ID)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.id").isEqualTo(ID)
                .jsonPath("$.url")
                .isEqualTo("https://example.com/rss.xml")
                .jsonPath("$.title").isEqualTo("Example")
                .jsonPath("$.createdAt")
                .isEqualTo("2026-10-07T07:00:00Z")
                .jsonPath("$").value(document -> {
                    Map<?, ?> response = (Map<?, ?>) document;
                    assertThat(response.containsKey("pollStatus")).isTrue();
                    assertThat(response.get("pollStatus")).isNull();
                });
    }

    @Test
    void returnsSanitizedPollStatusWithExplicitNullLastSuccess() {
        Feed feed = new Feed(
                ID,
                "https://example.com/rss.xml",
                "Example",
                CREATED_AT,
                new PollStatus(
                        Instant.parse("2026-10-28T08:00:00Z"),
                        null,
                        PollOutcome.FAILURE,
                        "TimeoutException",
                        2
                )
        );

        when(feedService.findById(ID)).thenReturn(Mono.just(feed));

        webTestClient.get()
                .uri("/feeds/{id}", ID)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.pollStatus.lastAttemptAt")
                .isEqualTo("2026-10-28T08:00:00Z")
                .jsonPath("$.pollStatus").value(value -> {
                    Map<?, ?> status = (Map<?, ?>) value;
                    assertThat(status.containsKey("lastSuccessAt")).isTrue();
                    assertThat(status.get("lastSuccessAt")).isNull();
                })
                .jsonPath("$.pollStatus.outcome").isEqualTo("FAILURE")
                .jsonPath("$.pollStatus.errorType")
                .isEqualTo("TimeoutException")
                .jsonPath("$.pollStatus.consecutiveFailures").isEqualTo(2)
                .jsonPath("$.pollStatus.message").doesNotExist()
                .jsonPath("$.pollStatus.url").doesNotExist();
    }

    @Test
    void returnsNotFoundForMissingFeed() {
        when(feedService.findById(ID))
                .thenReturn(Mono.error(
                        new FeedNotFoundException(ID)
                ));

        webTestClient.get()
                .uri("/feeds/{id}", ID)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo("feed not found: " + ID);
    }

    @Test
    void rejectsMalformedObjectId() {
        when(feedService.findById("abc"))
                .thenReturn(Mono.error(
                        new InvalidFeedIdException("abc")
                ));

        webTestClient.get()
                .uri("/feeds/abc")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo("id must be a valid ObjectId: abc");
    }

    @Test
    void itemPersistenceFailureReturnsApiError() {
        when(fetchFeedService.fetch(ID))
                .thenReturn(
                        Mono.error(
                                new ItemPersistenceException(
                                        new IllegalStateException(
                                                "database failed"
                                        )
                                )
                        )
                );

        webTestClient.post()
                .uri("/feeds/{id}/fetch", ID)
                .exchange()
                .expectStatus()
                .isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo("item persistence failed");
    }

    private void assertBadRequest(
            String requestBody,
            String expectedMessage
    ) {
        webTestClient.post()
                .uri("/feeds")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(requestBody)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo(expectedMessage);
    }
}
