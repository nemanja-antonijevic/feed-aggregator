package com.nantonijevic.feeds.integration;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.nantonijevic.feeds.TestcontainersConfiguration;
import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.domain.Item;
import com.nantonijevic.feeds.dto.ItemResponse;
import com.nantonijevic.feeds.repository.FeedRepository;
import com.nantonijevic.feeds.repository.ItemRepository;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.web.reactive.server.FluxExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "feeds.stream.heartbeat-interval=40ms",
                "feeds.stream.buffer-capacity=8"
        }
)
@AutoConfigureWebTestClient
@Import(TestcontainersConfiguration.class)
class ItemStreamIntegrationTest {

    private static final ParameterizedTypeReference<
            ServerSentEvent<ItemResponse>> ITEM_EVENT_TYPE =
            new ParameterizedTypeReference<>() {
            };

    private static Integer idleCursorBaseline;

    private static final String RSS = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
              <channel>
                <title>Stream source</title>
                <link>https://example.com</link>
                <description>Stream source</description>
                <item>
                  <guid>same-guid</guid>
                  <title>Same item</title>
                  <link>https://example.com/items/same</link>
                </item>
              </channel>
            </rss>
            """;

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private FeedRepository feedRepository;

    @Autowired
    private ItemRepository itemRepository;

    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;

    @BeforeEach
    void cleanDatabase() {
        StepVerifier.create(Mono.when(
                        feedRepository.deleteAll(),
                        itemRepository.deleteAll()
                ))
                .verifyComplete();
        if (idleCursorBaseline == null) {
            awaitStableCursorCount(Duration.ofSeconds(5));
            idleCursorBaseline = openCursorCount();
        } else {
            int expectedIdleCount = idleCursorBaseline;
            awaitCursorCount(
                    count -> count == expectedIdleCount,
                    Duration.ofSeconds(5)
            );
            awaitStableCursorCount(Duration.ofSeconds(5));
        }
    }

    @Test
    void sendsInsertedItemWithResumeTokenAndCompleteDto() {
        Feed feed = saveFeed("single-item");
        FluxExchangeResult<ServerSentEvent<ItemResponse>> response =
                openStream(feed.getId());

        Item saved = saveItem(feed, "first", Map.of("creator", "Ada"));
        ServerSentEvent<ItemResponse> event = nextItem(response);

        assertThat(event.event()).isEqualTo("item");
        assertThat(event.id()).isNotBlank();
        assertThat(BsonDocument.parse(event.id())).isNotEmpty();
        assertThat(event.data()).isEqualTo(new ItemResponse(
                saved.getId(),
                feed.getId(),
                "first",
                "Title first",
                "https://example.com/items/first",
                Instant.parse("2026-11-08T10:15:00Z"),
                Instant.parse("2026-11-09T12:00:00Z"),
                Map.of("creator", "Ada")
        ));
    }

    @Test
    void ignoresOtherFeedAndThenDeliversRequestedFeed() {
        Feed requested = saveFeed("requested");
        Feed other = saveFeed("other");
        FluxExchangeResult<ServerSentEvent<ItemResponse>> response =
                openStream(requested.getId());

        StepVerifier.create(itemFrames(response))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(180))
                .then(() -> saveItem(other, "other-item", Map.of()))
                .expectNoEvent(Duration.ofMillis(180))
                .then(() -> saveItem(requested, "requested-item", Map.of()))
                .assertNext(event -> assertThat(event.data().guid())
                        .isEqualTo("requested-item"))
                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void ignoresUpdatesAndStillDeliversLaterInserts() {
        Feed feed = saveFeed("insert-only");
        Item existing = saveItem(feed, "existing", Map.of());
        FluxExchangeResult<ServerSentEvent<ItemResponse>> response =
                openStream(feed.getId());

        StepVerifier.create(itemFrames(response))
                .expectSubscription()
                .then(() -> reactiveMongoTemplate.updateFirst(
                        Query.query(Criteria.where("id").is(existing.getId())),
                        Update.update("title", "updated"),
                        Item.class
                ).block(Duration.ofSeconds(3)))
                .expectNoEvent(Duration.ofMillis(180))
                .then(() -> saveItem(feed, "new-insert", Map.of()))
                .assertNext(event -> assertThat(event.data().guid())
                        .isEqualTo("new-insert"))
                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void duplicateFetchDoesNotEmitASecondInsert() throws Exception {
        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(rssResponse(RSS));
            source.enqueue(rssResponse(RSS));

            Feed feed = feedRepository.save(new Feed(
                    null,
                    source.url("/rss.xml").toString(),
                    "Duplicate source",
                    Instant.parse("2026-11-09T08:00:00Z")
            )).block();
            assertThat(feed).isNotNull();

            FluxExchangeResult<ServerSentEvent<ItemResponse>> response =
                    openStream(feed.getId());

            StepVerifier.create(itemFrames(response))
                    .expectSubscription()
                    .then(() -> webTestClient.post()
                            .uri("/feeds/{id}/fetch", feed.getId())
                            .exchange()
                            .expectStatus().isOk())
                    .assertNext(event -> assertThat(event.data().guid())
                            .isEqualTo("same-guid"))
                    .then(() -> webTestClient.post()
                            .uri("/feeds/{id}/fetch", feed.getId())
                            .exchange()
                            .expectStatus().isOk()
                            .expectBody()
                            .jsonPath("$.inserted").isEqualTo(0)
                            .jsonPath("$.duplicates").isEqualTo(1))
                    .expectNoEvent(Duration.ofMillis(220))
                    .thenCancel()
                    .verify(Duration.ofSeconds(6));
        }
    }

    @Test
    void resumesExclusivelyAfterLastEventId() {
        Feed feed = saveFeed("resume");
        FluxExchangeResult<ServerSentEvent<ItemResponse>> firstResponse =
                openStream(feed.getId());
        saveItem(feed, "item-1", Map.of());
        ServerSentEvent<ItemResponse> first = nextItem(firstResponse);

        saveItem(feed, "item-2", Map.of());
        saveItem(feed, "item-3", Map.of());

        FluxExchangeResult<ServerSentEvent<ItemResponse>> resumed =
                webTestClient.get()
                        .uri("/feeds/{id}/items/stream", feed.getId())
                        .header("Last-Event-ID", first.id())
                        .accept(MediaType.TEXT_EVENT_STREAM)
                        .exchange()
                        .expectStatus().isOk()
                        .returnResult(ITEM_EVENT_TYPE);

        List<ServerSentEvent<ItemResponse>> later = itemFrames(resumed)
                .take(2)
                .collectList()
                .block(Duration.ofSeconds(5));

        assertThat(later)
                .extracting(event -> event.data().guid())
                .containsExactly("item-2", "item-3");
        assertThat(later)
                .extracting(ServerSentEvent::id)
                .doesNotContain(first.id());
    }

    @Test
    void malformedIdsAndResumeTokensReturnJsonApiErrorsBeforeStreaming() {
        Feed feed = saveFeed("bad-token");

        webTestClient.get()
                .uri("/feeds/{id}/items/stream", feed.getId())
                .header("Last-Event-ID", "not-json")
                .exchange()
                .expectStatus().isBadRequest()
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo("Last-Event-ID must be a valid resume token");

        webTestClient.get()
                .uri("/feeds/abc/items/stream")
                .exchange()
                .expectStatus().isBadRequest()
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo("id must be a valid ObjectId: abc");

        String missingId = "507f1f77bcf86cd799439099";
        webTestClient.get()
                .uri("/feeds/{id}/items/stream", missingId)
                .exchange()
                .expectStatus().isNotFound()
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo("feed not found: " + missingId);
    }

    @Test
    void sendsHeartbeatCommentsOnAnIdleStream() {
        Feed feed = saveFeed("heartbeat");
        FluxExchangeResult<ServerSentEvent<ItemResponse>> response =
                openStream(feed.getId());

        StepVerifier.create(response.getResponseBody())
                .assertNext(event -> {
                    assertThat(event.comment()).isEqualTo("keepalive");
                    assertThat(event.data()).isNull();
                })
                .thenCancel()
                .verify(Duration.ofSeconds(3));
    }

    @Test
    void clientCancellationClosesTheMongoCursor() {
        Feed feed = saveFeed("cancel");
        FluxExchangeResult<ServerSentEvent<ItemResponse>> response =
                openStream(feed.getId());

        CountDownLatch firstSignal = new CountDownLatch(1);
        AtomicBoolean frameReceived = new AtomicBoolean();
        AtomicReference<Throwable> streamError = new AtomicReference<>();
        AtomicBoolean streamCompleted = new AtomicBoolean();
        Disposable subscription = response.getResponseBody()
                .subscribe(
                        ignored -> {
                            frameReceived.set(true);
                            firstSignal.countDown();
                        },
                        error -> {
                            streamError.set(error);
                            firstSignal.countDown();
                        },
                        () -> {
                            streamCompleted.set(true);
                            firstSignal.countDown();
                        }
                );

        try {
            assertThat(firstSignal.await(5, TimeUnit.SECONDS))
                    .as("SSE stream emits a frame or terminates")
                    .isTrue();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AssertionError("waiting for the first SSE signal was interrupted", exception);
        }

        assertThat(frameReceived.get())
                .as("the stream emitted a frame before client cancellation")
                .isTrue();
        assertThat(streamError.get())
                .as("the stream had not errored before client cancellation")
                .isNull();
        assertThat(streamCompleted.get())
                .as("the stream had not completed before client cancellation")
                .isFalse();

        subscription.dispose();
        int expectedIdleCount = idleCursorBaseline;
        awaitCursorCount(
                count -> count == expectedIdleCount,
                Duration.ofSeconds(5),
                "idle cursor baseline " + expectedIdleCount
        );
    }

    private FluxExchangeResult<ServerSentEvent<ItemResponse>> openStream(
            String feedId
    ) {
        int baseline = openCursorCount();
        FluxExchangeResult<ServerSentEvent<ItemResponse>> response =
                webTestClient.get()
                        .uri("/feeds/{id}/items/stream", feedId)
                        .accept(MediaType.TEXT_EVENT_STREAM)
                        .exchange()
                        .expectStatus().isOk()
                        .expectHeader()
                        .contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                        .returnResult(ITEM_EVENT_TYPE);

        try {
            awaitCursorCount(count -> count > baseline, Duration.ofSeconds(5));
        } catch (AssertionError error) {
            throw new AssertionError(
                    "stream cursor did not exceed baseline " + baseline,
                    error
            );
        }
        return response;
    }

    private void awaitStableCursorCount(Duration timeout) {
        AtomicReference<Integer> previous = new AtomicReference<>();
        AtomicInteger stableSamples = new AtomicInteger();

        awaitCursorCount(count -> {
            Integer prior = previous.getAndSet(count);
            if (prior != null && prior == count) {
                return stableSamples.incrementAndGet() >= 17;
            }
            stableSamples.set(1);
            return false;
        }, timeout);
    }

    private Flux<ServerSentEvent<ItemResponse>> itemFrames(
            FluxExchangeResult<ServerSentEvent<ItemResponse>> response
    ) {
        return response.getResponseBody()
                .filter(event -> event.data() != null);
    }

    private ServerSentEvent<ItemResponse> nextItem(
            FluxExchangeResult<ServerSentEvent<ItemResponse>> response
    ) {
        ServerSentEvent<ItemResponse> event = itemFrames(response)
                .next()
                .block(Duration.ofSeconds(5));
        assertThat(event).isNotNull();
        return event;
    }

    private MockResponse rssResponse(String body) {
        return new MockResponse.Builder()
                .code(200)
                .addHeader("Content-Type", "application/rss+xml")
                .body(body)
                .build();
    }

    private Feed saveFeed(String name) {
        Feed feed = feedRepository.save(new Feed(
                null,
                "https://example.com/stream/" + name + ".xml",
                "Stream " + name,
                Instant.parse("2026-11-09T08:00:00Z")
        )).block();
        assertThat(feed).isNotNull();
        return feed;
    }

    private Item saveItem(
            Feed feed,
            String guid,
            Map<String, Object> extras
    ) {
        Item item = itemRepository.save(new Item(
                null,
                feed.getId(),
                guid,
                "Title " + guid,
                "https://example.com/items/" + guid,
                Instant.parse("2026-11-08T10:15:00Z"),
                Instant.parse("2026-11-09T12:00:00Z"),
                extras
        )).block();
        assertThat(item).isNotNull();
        return item;
    }

    private int openCursorCount() {
        Document status = reactiveMongoTemplate
                .executeCommand(new Document("serverStatus", 1))
                .block(Duration.ofSeconds(3));
        assertThat(status).isNotNull();
        Document metrics = status.get("metrics", Document.class);
        Document cursors = metrics.get("cursor", Document.class);
        Document open = cursors.get("open", Document.class);
        return open.get("total", Number.class).intValue();
    }

    private void awaitCursorCount(
            java.util.function.IntPredicate condition,
            Duration timeout
    ) {
        awaitCursorCount(condition, timeout, "the requested cursor count");
    }

    private void awaitCursorCount(
            java.util.function.IntPredicate condition,
            Duration timeout,
            String expectedCount
    ) {
        long deadline = System.nanoTime() + timeout.toNanos();
        int current;
        do {
            current = openCursorCount();
            if (condition.test(current)) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new AssertionError("cursor polling was interrupted", exception);
            }
        } while (System.nanoTime() < deadline);

        throw new AssertionError(
                "cursor count did not reach expected " + expectedCount
                        + " before timeout; expected " + expectedCount
                        + ", observed " + current
        );
    }
}
