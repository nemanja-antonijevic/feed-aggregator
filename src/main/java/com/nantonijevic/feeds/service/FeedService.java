package com.nantonijevic.feeds.service;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.domain.Item;
import com.nantonijevic.feeds.dto.DailyItemCountResponse;
import com.nantonijevic.feeds.exception.DuplicateFeedUrlException;
import com.nantonijevic.feeds.exception.FeedNotFoundException;
import com.nantonijevic.feeds.exception.InvalidFeedIdException;
import com.nantonijevic.feeds.repository.FeedRepository;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class FeedService {

    private final FeedRepository feedRepository;
    private final Clock clock;
    private final ReactiveMongoTemplate reactiveMongoTemplate;

    public FeedService(
            FeedRepository feedRepository,
            Clock clock,
            ReactiveMongoTemplate reactiveMongoTemplate
    ) {
        this.feedRepository = feedRepository;
        this.clock = clock;
        this.reactiveMongoTemplate = reactiveMongoTemplate;
    }

    public Mono<Feed> create(String url, String title) {
        Feed feed = new Feed(
                null,
                url,
                title,
                Instant.now(clock).truncatedTo(ChronoUnit.MILLIS)
        );

        return feedRepository.save(feed)
                .onErrorMap(
                        DuplicateKeyException.class,
                        DuplicateFeedUrlException::new
                );
    }

    public Mono<Feed> findById(String id) {
        if (!ObjectId.isValid(id)) {
            return Mono.error(new InvalidFeedIdException(id));
        }

        return feedRepository.findById(id)
                .switchIfEmpty(Mono.defer(() ->
                        Mono.error(new FeedNotFoundException(id))
                ));
    }

    public Flux<Feed> findAll() {
        Sort sort = Sort.by(
                Sort.Order.desc("createdAt"),
                Sort.Order.desc("id")
        );

        return feedRepository.findAll(sort);
    }

    public Flux<DailyItemCountResponse> dailyItemCounts(String feedId) {
        return findById(feedId)
                .thenMany(reactiveMongoTemplate.aggregate(
                        dailyItemCountsAggregation(feedId),
                        Item.class,
                        DailyItemCount.class
                ))
                .map(count -> new DailyItemCountResponse(
                        feedId,
                        count.day(),
                        count.count()
                ));
    }

    public Mono<Void> deleteById(String id) {
        if (!ObjectId.isValid(id)) {
            return Mono.error(new InvalidFeedIdException(id));
        }

        Query query = Query.query(
                Criteria.where("id").is(id)
        );

        return reactiveMongoTemplate.remove(query, Feed.class)
                .filter(result -> result.getDeletedCount() == 1)
                .switchIfEmpty(Mono.defer(() ->
                        Mono.error(new FeedNotFoundException(id))
                ))
                .then();
    }

    Aggregation dailyItemCountsAggregation(String feedId) {
        Document effectiveDate = new Document(
                "$ifNull",
                List.of("$publishedAt", "$fetchedAt")
        );
        Document day = new Document("$dateToString", new Document()
                .append("date", effectiveDate)
                .append("format", "%Y-%m-%d")
                .append("timezone", "UTC"));

        return Aggregation.newAggregation(
                Aggregation.match(
                        Criteria.where("feedId").is(feedId)
                ),
                context -> new Document(
                        "$project",
                        new Document("day", day)
                                .append("_id", 0)
                ),
                Aggregation.group("day")
                        .count()
                        .as("count"),
                Aggregation.sort(
                        Sort.Direction.ASC,
                        "_id"
                ),
                Aggregation.project("count")
                        .and("_id")
                        .as("day")
                        .andExclude("_id")
        );
    }

    private record DailyItemCount(String day, long count) {
    }
}
