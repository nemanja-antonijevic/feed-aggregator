package com.nantonijevic.feeds.service;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import com.nantonijevic.feeds.client.FeedSourceClient;
import com.nantonijevic.feeds.domain.Item;
import com.nantonijevic.feeds.dto.FetchFeedResponse;
import com.nantonijevic.feeds.exception.ItemPersistenceException;
import com.nantonijevic.feeds.parser.FeedParser;
import com.nantonijevic.feeds.parser.ParsedFeedItem;
import com.nantonijevic.feeds.repository.ItemRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
public class FetchFeedService {

    private static final int INSERT_CONCURRENCY = 4;

    private final FeedService feedService;
    private final FeedSourceClient feedSourceClient;
    private final FeedParser feedParser;
    private final ItemRepository itemRepository;
    private final Clock clock;

    public FetchFeedService(
            FeedService feedService,
            FeedSourceClient feedSourceClient,
            FeedParser feedParser,
            ItemRepository itemRepository,
            Clock clock
    ) {
        this.feedService = feedService;
        this.feedSourceClient = feedSourceClient;
        this.feedParser = feedParser;
        this.itemRepository = itemRepository;
        this.clock = clock;
    }

    public Mono<FetchFeedResponse> fetch(String feedId) {
        return feedService.findById(feedId)
                .flatMap(feed ->
                        feedSourceClient.fetch(feed.getUrl())
                                .flatMap(this::parse)
                                .flatMap(items ->
                                        storeItems(feed.getId(), items)
                                )
                );
    }

    private Mono<List<ParsedFeedItem>> parse(byte[] body) {
        return Mono.fromCallable(() -> feedParser.parse(body))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<FetchFeedResponse> storeItems(
            String feedId,
            List<ParsedFeedItem> parsedItems
    ) {
        Instant fetchedAt = Instant.now(clock)
                .truncatedTo(ChronoUnit.MILLIS);

        return Flux.fromIterable(parsedItems)
                .flatMap(
                        parsedItem ->
                                storeItem(
                                        feedId,
                                        parsedItem,
                                        fetchedAt
                                ),
                        INSERT_CONCURRENCY
                )
                .reduce(
                        new SaveOutcome(0, 0),
                        SaveOutcome::add
                )
                .map(outcome ->
                        new FetchFeedResponse(
                                parsedItems.size(),
                                outcome.inserted(),
                                outcome.duplicates()
                        )
                );
    }

    private Mono<SaveOutcome> storeItem(
            String feedId,
            ParsedFeedItem parsedItem,
            Instant fetchedAt
    ) {
        Item item = new Item(
                null,
                feedId,
                parsedItem.guid(),
                parsedItem.title(),
                parsedItem.link(),
                parsedItem.publishedAt(),
                fetchedAt
        );

        return itemRepository.save(item)
                .map(saved -> new SaveOutcome(1, 0))
                .onErrorResume(
                        DuplicateKeyException.class,
                        exception ->
                                Mono.just(new SaveOutcome(0, 1))
                )
                .onErrorMap(ItemPersistenceException::new);
    }

    private record SaveOutcome(
            int inserted,
            int duplicates
    ) {
        SaveOutcome add(SaveOutcome other) {
            return new SaveOutcome(
                    inserted + other.inserted,
                    duplicates + other.duplicates
            );
        }
    }
}
