package com.nantonijevic.feeds.integration;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.nantonijevic.feeds.TestcontainersConfiguration;
import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.domain.Item;
import com.nantonijevic.feeds.dto.FeedResponse;
import com.nantonijevic.feeds.repository.FeedRepository;
import com.nantonijevic.feeds.repository.ItemRepository;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.SocketEffect;
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

    @Autowired
    private ItemRepository itemRepository;

    @BeforeEach
    void cleanDatabase() {
        StepVerifier.create(
                Mono.when(
                        feedRepository.deleteAll(),
                        itemRepository.deleteAll()
                )
        ).verifyComplete();
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

    @Test
    void fetchesAndStoresItemsFromRssFeed() throws IOException {
        String rss = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
              <channel>
                <title>Example feed</title>
                <link>https://example.com</link>
                <description>Example feed</description>
                <item>
                  <guid>item-1</guid>
                  <title>First item</title>
                  <link>https://example.com/items/1</link>
                  <pubDate>Wed, 08 Oct 2025 10:00:00 GMT</pubDate>
                </item>
                <item>
                  <guid>item-2</guid>
                  <title>Second item</title>
                  <link>https://example.com/items/2</link>
                  <pubDate>Wed, 08 Oct 2025 11:00:00 GMT</pubDate>
                </item>
              </channel>
            </rss>
            """;

        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(
                    new MockResponse.Builder()
                            .code(200)
                            .addHeader(
                                    "Content-Type",
                                    "application/rss+xml"
                            )
                            .body(rss)
                            .build()
            );

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/rss.xml").toString(),
                            "Example",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus().isOk()
                    .expectHeader()
                    .contentType(MediaType.APPLICATION_JSON)
                    .expectBody()
                    .jsonPath("$.fetched").isEqualTo(2)
                    .jsonPath("$.inserted").isEqualTo(2)
                    .jsonPath("$.duplicates").isEqualTo(0);

            StepVerifier.create(
                            reactiveMongoTemplate
                                    .getCollection("items")
                                    .flatMap(collection ->
                                            Mono.from(
                                                    collection.countDocuments()
                                            )
                                    )
                    )
                    .expectNext(2L)
                    .verifyComplete();
        }
    }

    @Test
    void createsCompoundUniqueIndexForFeedItems() {
        StepVerifier.create(
                        reactiveMongoTemplate
                                .indexOps(Item.class)
                                .getIndexInfo()
                                .filter(index ->
                                        "feed_id_guid_unique"
                                                .equals(index.getName())
                                )
                                .single()
                )
                .assertNext(index -> {
                    assertThat(index.isUnique()).isTrue();
                    assertThat(index.getIndexFields())
                            .extracting(IndexField::getKey)
                            .containsExactly("feedId", "guid");
                })
                .verifyComplete();
    }

    @Test
    void repeatedFetchStoresNoDuplicateItems() throws IOException {
        String rss = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
              <channel>
                <title>Example feed</title>
                <link>https://example.com</link>
                <description>Example feed</description>
                <item>
                  <guid>same-item</guid>
                  <title>Same item</title>
                  <link>https://example.com/items/same</link>
                </item>
              </channel>
            </rss>
            """;

        try (MockWebServer source = new MockWebServer()) {
            source.start();

            source.enqueue(rssResponse(rss));
            source.enqueue(rssResponse(rss));

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/rss.xml").toString(),
                            "Example",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.fetched").isEqualTo(1)
                    .jsonPath("$.inserted").isEqualTo(1)
                    .jsonPath("$.duplicates").isEqualTo(0);

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.fetched").isEqualTo(1)
                    .jsonPath("$.inserted").isEqualTo(0)
                    .jsonPath("$.duplicates").isEqualTo(1);

            StepVerifier.create(itemRepository.count())
                    .expectNext(1L)
                    .verifyComplete();
        }
    }

    private MockResponse rssResponse(String body) {
        return new MockResponse.Builder()
                .code(200)
                .addHeader(
                        "Content-Type",
                        "application/rss+xml"
                )
                .body(body)
                .build();
    }

    @Test
    void duplicateGuidInSameResponseIsCountedAsDuplicate()
            throws IOException {
        String rss = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
              <channel>
                <title>Example feed</title>
                <link>https://example.com</link>
                <description>Example feed</description>
                <item>
                  <guid>duplicate-guid</guid>
                  <title>First version</title>
                  <link>https://example.com/items/first</link>
                </item>
                <item>
                  <guid>duplicate-guid</guid>
                  <title>Second version</title>
                  <link>https://example.com/items/second</link>
                </item>
              </channel>
            </rss>
            """;

        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(rssResponse(rss));

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/rss.xml").toString(),
                            "Example",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.fetched").isEqualTo(2)
                    .jsonPath("$.inserted").isEqualTo(1)
                    .jsonPath("$.duplicates").isEqualTo(1);

            StepVerifier.create(itemRepository.count())
                    .expectNext(1L)
                    .verifyComplete();
        }
    }

    @Test
    void sameGuidInDifferentFeedsIsStoredTwice()
            throws IOException {
        String rss = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
              <channel>
                <title>Example feed</title>
                <link>https://example.com</link>
                <description>Example feed</description>
                <item>
                  <guid>shared-guid</guid>
                  <title>Shared item</title>
                  <link>https://example.com/items/shared</link>
                </item>
              </channel>
            </rss>
            """;

        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(rssResponse(rss));
            source.enqueue(rssResponse(rss));

            Feed firstFeed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/first.xml").toString(),
                            "First feed",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            Feed secondFeed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/second.xml").toString(),
                            "Second feed",
                            Instant.parse("2026-10-09T08:01:00Z")
                    )
            ).block();

            assertThat(firstFeed).isNotNull();
            assertThat(secondFeed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", firstFeed.getId())
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.inserted").isEqualTo(1)
                    .jsonPath("$.duplicates").isEqualTo(0);

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", secondFeed.getId())
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.inserted").isEqualTo(1)
                    .jsonPath("$.duplicates").isEqualTo(0);

            StepVerifier.create(itemRepository.count())
                    .expectNext(2L)
                    .verifyComplete();
        }
    }

    @Test
    void fetchesItemsFromAtomFeed() throws IOException {
        String atom = """
            <?xml version="1.0" encoding="UTF-8"?>
            <feed xmlns="http://www.w3.org/2005/Atom">
              <title>Example Atom feed</title>
              <id>urn:example:feed</id>
              <updated>2026-10-09T08:00:00Z</updated>
              <entry>
                <title>Atom item</title>
                <id>urn:example:item:1</id>
                <link href="https://example.com/atom/1"/>
                <updated>2026-10-09T08:30:00Z</updated>
              </entry>
            </feed>
            """;

        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(
                    new MockResponse.Builder()
                            .code(200)
                            .addHeader(
                                    "Content-Type",
                                    "application/atom+xml"
                            )
                            .body(atom)
                            .build()
            );

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/atom.xml").toString(),
                            "Atom",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.fetched").isEqualTo(1)
                    .jsonPath("$.inserted").isEqualTo(1)
                    .jsonPath("$.duplicates").isEqualTo(0);

            StepVerifier.create(itemRepository.findAll())
                    .assertNext(item -> {
                        assertThat(item.getGuid())
                                .isEqualTo("urn:example:item:1");
                        assertThat(item.getTitle())
                                .isEqualTo("Atom item");
                        assertThat(item.getLink())
                                .isEqualTo(
                                        "https://example.com/atom/1"
                                );
                    })
                    .verifyComplete();
        }
    }

    @Test
    void storesItemWithoutTitleAndUsesLinkAsGuid()
            throws IOException {
        String rss = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
              <channel>
                <title>Example feed</title>
                <link>https://example.com</link>
                <description>Example feed</description>
                <item>
                  <link>https://example.com/items/link-only</link>
                  <pubDate>not-a-date</pubDate>
                </item>
              </channel>
            </rss>
            """;

        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(rssResponse(rss));

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/rss.xml").toString(),
                            "Example",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.fetched").isEqualTo(1)
                    .jsonPath("$.inserted").isEqualTo(1)
                    .jsonPath("$.duplicates").isEqualTo(0);

            StepVerifier.create(itemRepository.findAll())
                    .assertNext(item -> {
                        assertThat(item.getGuid())
                                .isEqualTo(
                                        "https://example.com/items/link-only"
                                );
                        assertThat(item.getTitle()).isNull();
                        assertThat(item.getPublishedAt()).isNull();
                        assertThat(item.getFetchedAt()).isNotNull();
                    })
                    .verifyComplete();
        }
    }

    @Test
    void itemWithoutGuidAndLinkRejectsWholeFeed()
            throws IOException {
        String rss = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
              <channel>
                <title>Example feed</title>
                <link>https://example.com</link>
                <description>Example feed</description>
                <item>
                  <guid>valid-item</guid>
                  <title>Valid item</title>
                  <link>https://example.com/items/valid</link>
                </item>
                <item>
                  <title>Missing identity</title>
                </item>
              </channel>
            </rss>
            """;

        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(rssResponse(rss));

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/rss.xml").toString(),
                            "Example",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus()
                    .isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT)
                    .expectBody()
                    .jsonPath("$.message")
                    .isEqualTo(
                            "feed source returned an invalid feed"
                    );

            StepVerifier.create(itemRepository.count())
                    .expectNext(0L)
                    .verifyComplete();
        }
    }

    @Test
    void upstreamServerErrorReturnsBadGatewayAndStoresNothing()
            throws IOException {
        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(
                    new MockResponse.Builder()
                            .code(500)
                            .addHeader(
                                    "Content-Type",
                                    "text/plain"
                            )
                            .body("source failed")
                            .build()
            );

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/rss.xml").toString(),
                            "Example",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus()
                    .isEqualTo(HttpStatus.BAD_GATEWAY)
                    .expectBody()
                    .jsonPath("$.message")
                    .isEqualTo(
                            "feed source returned HTTP 500"
                    );

            StepVerifier.create(itemRepository.count())
                    .expectNext(0L)
                    .verifyComplete();
        }
    }

    @Test
    void malformedXmlReturnsUnprocessableContentAndStoresNothing()
            throws IOException {
        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(
                    new MockResponse.Builder()
                            .code(200)
                            .addHeader(
                                    "Content-Type",
                                    "application/rss+xml"
                            )
                            .body("<rss><channel>")
                            .build()
            );

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/broken.xml").toString(),
                            "Broken",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus()
                    .isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT)
                    .expectBody()
                    .jsonPath("$.message")
                    .isEqualTo(
                            "feed source returned an invalid feed"
                    );

            StepVerifier.create(itemRepository.count())
                    .expectNext(0L)
                    .verifyComplete();
        }
    }

    @Test
    void htmlResponseReturnsUnprocessableContentAndStoresNothing()
            throws IOException {
        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(
                    new MockResponse.Builder()
                            .code(200)
                            .addHeader(
                                    "Content-Type",
                                    "text/html; charset=UTF-8"
                            )
                            .body("""
                                <html>
                                  <body>Not a feed</body>
                                </html>
                                """)
                            .build()
            );

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/feed").toString(),
                            "HTML source",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus()
                    .isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT)
                    .expectBody()
                    .jsonPath("$.message")
                    .isEqualTo(
                            "feed source did not return an RSS or Atom feed"
                    );

            StepVerifier.create(itemRepository.count())
                    .expectNext(0L)
                    .verifyComplete();
        }
    }

    @Test
    void sourceTimeoutReturnsGatewayTimeoutAndStoresNothing()
            throws IOException {
        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(
                    new MockResponse.Builder()
                            .code(200)
                            .headersDelay(4, TimeUnit.SECONDS)
                            .addHeader(
                                    "Content-Type",
                                    "application/rss+xml"
                            )
                            .body("<rss version=\"2.0\"/>")
                            .build()
            );

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/slow.xml").toString(),
                            "Slow",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus()
                    .isEqualTo(HttpStatus.GATEWAY_TIMEOUT)
                    .expectBody()
                    .jsonPath("$.message")
                    .isEqualTo("feed source timed out");

            StepVerifier.create(itemRepository.count())
                    .expectNext(0L)
                    .verifyComplete();
        }
    }

    @Test
    void slowContinuousBodyReturnsGatewayTimeoutAndStoresNothing()
            throws IOException {
        String rss = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
              <channel>
                <title>Slow feed</title>
                <link>https://example.com</link>
                <description>Slow feed</description>
              </channel>
            </rss>
            """;

        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(
                    new MockResponse.Builder()
                            .code(200)
                            .addHeader(
                                    "Content-Type",
                                    "application/rss+xml"
                            )
                            .body(rss)
                            .throttleBody(16, 1, TimeUnit.SECONDS)
                            .build()
            );

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/slow-body.xml").toString(),
                            "Slow body",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus()
                    .isEqualTo(HttpStatus.GATEWAY_TIMEOUT)
                    .expectBody()
                    .jsonPath("$.message")
                    .isEqualTo("feed source timed out");

            StepVerifier.create(itemRepository.count())
                    .expectNext(0L)
                    .verifyComplete();
        }
    }

    @Test
    void disconnectDuringResponseBodyReturnsBadGatewayAndStoresNothing()
            throws IOException {
        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(
                    new MockResponse.Builder()
                            .code(200)
                            .addHeader(
                                    "Content-Type",
                                    "application/rss+xml"
                            )
                            .body("<rss version=\"2.0\"><channel>")
                            .onResponseBody(
                                    new SocketEffect.CloseSocket()
                            )
                            .build()
            );

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/disconnect.xml").toString(),
                            "Disconnecting source",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus()
                    .isEqualTo(HttpStatus.BAD_GATEWAY)
                    .expectBody()
                    .jsonPath("$.message")
                    .isEqualTo("feed source request failed");

            StepVerifier.create(itemRepository.count())
                    .expectNext(0L)
                    .verifyComplete();
        }
    }

    @Test
    void oversizedSourceBodyReturnsBadGatewayAndStoresNothing()
            throws IOException {
        String oversizedBody = "a".repeat(1024 * 1024 + 1);

        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(
                    new MockResponse.Builder()
                            .code(200)
                            .addHeader(
                                    "Content-Type",
                                    "application/rss+xml"
                            )
                            .body(oversizedBody)
                            .build()
            );

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/large.xml").toString(),
                            "Large",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus()
                    .isEqualTo(HttpStatus.BAD_GATEWAY)
                    .expectBody()
                    .jsonPath("$.message")
                    .isEqualTo(
                            "feed source body exceeds 1 MiB"
                    );

            StepVerifier.create(itemRepository.count())
                    .expectNext(0L)
                    .verifyComplete();
        }
    }

    @Test
    void upstreamNotFoundReturnsBadGatewayAndStoresNothing()
            throws IOException {
        try (MockWebServer source = new MockWebServer()) {
            source.start();
            source.enqueue(
                    new MockResponse.Builder()
                            .code(404)
                            .build()
            );

            Feed feed = feedRepository.save(
                    new Feed(
                            null,
                            source.url("/missing.xml").toString(),
                            "Missing source",
                            Instant.parse("2026-10-09T08:00:00Z")
                    )
            ).block();

            assertThat(feed).isNotNull();

            webTestClient.post()
                    .uri("/feeds/{id}/fetch", feed.getId())
                    .exchange()
                    .expectStatus()
                    .isEqualTo(HttpStatus.BAD_GATEWAY)
                    .expectBody()
                    .jsonPath("$.message")
                    .isEqualTo(
                            "feed source returned HTTP 404"
                    );

            StepVerifier.create(itemRepository.count())
                    .expectNext(0L)
                    .verifyComplete();
        }
    }

    @Test
    void fetchingMissingFeedReturnsNotFound() {
        String id = "507f1f77bcf86cd799439099";

        webTestClient.post()
                .uri("/feeds/{id}/fetch", id)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.message")
                .isEqualTo("feed not found: " + id);

        StepVerifier.create(itemRepository.count())
                .expectNext(0L)
                .verifyComplete();
    }
}
