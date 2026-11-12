package com.nantonijevic.feeds.service;

import java.time.Duration;

import com.mongodb.MongoException;
import com.nantonijevic.feeds.config.FeedStreamProperties;
import com.nantonijevic.feeds.domain.Item;
import com.nantonijevic.feeds.dto.ItemResponse;
import com.nantonijevic.feeds.exception.ChangeStreamHistoryLostException;
import com.nantonijevic.feeds.exception.InvalidResumeTokenException;
import com.nantonijevic.feeds.exception.ItemStreamBackpressureException;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.Exceptions;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Service
public class ItemStreamService {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ItemStreamService.class);

    private static final int CHANGE_STREAM_HISTORY_LOST_CODE = 286;

    private final ReactiveMongoTemplate reactiveMongoTemplate;
    private final FeedStreamProperties properties;

    public ItemStreamService(
            ReactiveMongoTemplate reactiveMongoTemplate,
            FeedStreamProperties properties
    ) {
        this.reactiveMongoTemplate = reactiveMongoTemplate;
        this.properties = properties;
    }

    public Flux<ServerSentEvent<Object>> stream(
            String feedId,
            String lastEventId
    ) {
        return withHeartbeats(itemEvents(feedId, lastEventId));
    }

    Flux<ServerSentEvent<Object>> withHeartbeats(
            Flux<ServerSentEvent<Object>> items
    ) {
        Sinks.One<Void> itemsDone = Sinks.one();
        Flux<ServerSentEvent<Object>> observedItems = items
                .doFinally(ignored -> itemsDone.tryEmitEmpty());

        Duration heartbeatInterval = properties.getHeartbeatInterval();
        Flux<ServerSentEvent<Object>> heartbeats = Flux
                .interval(heartbeatInterval)
                .map(ignored -> ServerSentEvent.builder()
                        .comment("keepalive")
                        .build())
                .onBackpressureLatest()
                .takeUntilOther(itemsDone.asMono());

        return Flux.merge(1, observedItems, heartbeats);
    }

    public Flux<ServerSentEvent<Object>> itemEvents(
            String feedId,
            String lastEventId
    ) {
        BsonDocument resumeToken = parseResumeToken(lastEventId);
        Aggregation filter = Aggregation.newAggregation(
                Aggregation.match(
                        Criteria.where("operationType").is("insert")
                                .and("fullDocument.feedId")
                                .is(new ObjectId(feedId))
                )
        );

        var changeStream = reactiveMongoTemplate
                .changeStream(Item.class)
                .watchCollection("items")
                .filter(filter);

        Flux<ChangeStreamEvent<Item>> changeEvents = resumeToken == null
                ? changeStream.listen()
                : changeStream.resumeAfter(resumeToken).listen();
        return boundedItemEvents(
                        changeEvents.map(this::toItemEvent),
                        feedId
                )
                .doOnCancel(() -> {
                    LOGGER.debug("Item stream cancelled for feed {}", feedId);
                });
    }

    Flux<ServerSentEvent<Object>> boundedItemEvents(
            Flux<ServerSentEvent<Object>> itemEvents,
            String feedId
    ) {
        return itemEvents
                .onBackpressureBuffer(
                        properties.getBufferCapacity(),
                        ignored -> LOGGER.warn(
                                "Live item stream buffer overflowed for feed {}",
                                feedId
                        ),
                        BufferOverflowStrategy.ERROR
                )
                .onErrorMap(
                        Exceptions::isOverflow,
                        error -> new ItemStreamBackpressureException(
                                properties.getBufferCapacity(),
                                error
                        )
                )
                .onErrorMap(this::mapStreamError);
    }

    private ServerSentEvent<Object> toItemEvent(
            ChangeStreamEvent<Item> event
    ) {
        Item item = event.getBody();
        if (item == null) {
            throw new IllegalStateException(
                    "insert change event did not contain a full document"
            );
        }

        return ServerSentEvent.builder((Object) ItemResponse.from(item))
                .id(event.getResumeToken().asDocument().toJson())
                .event("item")
                .build();
    }

    private BsonDocument parseResumeToken(String lastEventId) {
        if (lastEventId == null || lastEventId.isBlank()) {
            return null;
        }

        try {
            return BsonDocument.parse(lastEventId);
        } catch (RuntimeException exception) {
            throw new InvalidResumeTokenException(exception);
        }
    }

    private Throwable mapStreamError(Throwable error) {
        Throwable unwrapped = Exceptions.unwrap(error);
        if (unwrapped instanceof MongoException mongoException
                && mongoException.getCode()
                == CHANGE_STREAM_HISTORY_LOST_CODE) {
            return new ChangeStreamHistoryLostException(unwrapped);
        }

        return unwrapped;
    }
}
