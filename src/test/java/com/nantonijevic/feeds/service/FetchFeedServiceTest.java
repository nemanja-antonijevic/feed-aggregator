package com.nantonijevic.feeds.service;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.nantonijevic.feeds.client.FeedSourceClient;
import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.domain.Item;
import com.nantonijevic.feeds.exception.ItemPersistenceException;
import com.nantonijevic.feeds.parser.FeedParser;
import com.nantonijevic.feeds.parser.ParsedFeedItem;
import com.nantonijevic.feeds.repository.ItemRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FetchFeedServiceTest {

    private static final String FEED_ID =
            "507f1f77bcf86cd799439011";

    private static final String FEED_URL =
            "https://example.com/rss.xml";

    private static final Instant NOW =
            Instant.parse("2026-10-09T08:00:00Z");

    @Mock
    private FeedService feedService;

    @Mock
    private FeedSourceClient feedSourceClient;

    @Mock
    private FeedParser feedParser;

    @Mock
    private ItemRepository itemRepository;

    private FetchFeedService fetchFeedService;

    @BeforeEach
    void setUp() {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

        fetchFeedService = new FetchFeedService(
                feedService,
                feedSourceClient,
                feedParser,
                itemRepository,
                clock
        );
    }

    @Test
    void mapsUnexpectedRepositoryFailure() {
        byte[] body = "<rss/>".getBytes();
        IllegalStateException repositoryFailure =
                new IllegalStateException("database failed");

        Feed feed = new Feed(
                FEED_ID,
                FEED_URL,
                "Example",
                NOW
        );

        ParsedFeedItem parsedItem = new ParsedFeedItem(
                "item-1",
                "Item",
                "https://example.com/items/1",
                NOW
        );

        when(feedService.findById(FEED_ID))
                .thenReturn(Mono.just(feed));

        when(feedSourceClient.fetch(FEED_URL))
                .thenReturn(Mono.just(body));

        when(feedParser.parse(body))
                .thenReturn(List.of(parsedItem));

        when(itemRepository.save(any(Item.class)))
                .thenReturn(Mono.error(repositoryFailure));

        StepVerifier.create(fetchFeedService.fetch(FEED_ID))
                .expectErrorSatisfies(error ->
                        assertThat(error)
                                .isInstanceOf(
                                        ItemPersistenceException.class
                                )
                                .hasCause(repositoryFailure)
                )
                .verify();
    }

    @Test
    void parsesOnBoundedElasticThread() {
        byte[] body = "<rss/>".getBytes();
        AtomicReference<String> parserThread =
                new AtomicReference<>();

        Feed feed = new Feed(
                FEED_ID,
                FEED_URL,
                "Example",
                NOW
        );

        when(feedService.findById(FEED_ID))
                .thenReturn(Mono.just(feed));

        when(feedSourceClient.fetch(FEED_URL))
                .thenReturn(Mono.just(body));

        when(feedParser.parse(body))
                .thenAnswer(invocation -> {
                    parserThread.set(
                            Thread.currentThread().getName()
                    );
                    return List.of();
                });

        StepVerifier.create(fetchFeedService.fetch(FEED_ID))
                .assertNext(response -> {
                    assertThat(response.fetched()).isZero();
                    assertThat(response.inserted()).isZero();
                    assertThat(response.duplicates()).isZero();
                })
                .verifyComplete();

        assertThat(parserThread.get())
                .startsWith("boundedElastic-");
    }
}
