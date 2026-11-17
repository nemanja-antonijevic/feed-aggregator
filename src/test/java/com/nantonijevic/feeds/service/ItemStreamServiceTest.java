package com.nantonijevic.feeds.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.nantonijevic.feeds.config.FeedStreamProperties;
import com.nantonijevic.feeds.dto.ItemResponse;
import com.nantonijevic.feeds.exception.ChangeStreamHistoryLostException;
import com.nantonijevic.feeds.exception.ItemStreamBackpressureException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.http.codec.ServerSentEvent;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ItemStreamServiceTest {

    @Test
    void heartbeatFluxCompletesWhenItemFluxCompletes() {
        FeedStreamProperties properties = new FeedStreamProperties();
        properties.setHeartbeatInterval(Duration.ofMillis(20));
        ItemStreamService service = new ItemStreamService(
                mock(ReactiveMongoTemplate.class),
                properties
        );

        StepVerifier.create(service.withHeartbeats(Flux.empty()))
                .expectComplete()
                .verify(Duration.ofSeconds(3));
    }

    @Test
    void boundedItemBufferErrorsWithoutDroppingItsQueuedPrefix() {
        int capacity = 8;
        FeedStreamProperties properties = new FeedStreamProperties();
        properties.setBufferCapacity(capacity);
        ItemStreamService service = new ItemStreamService(
                mock(ReactiveMongoTemplate.class),
                properties
        );
        TestPublisher<ServerSentEvent<Object>> upstream =
                TestPublisher.createNoncompliant(
                        TestPublisher.Violation.REQUEST_OVERFLOW
                );
        List<ServerSentEvent<Object>> delivered = new ArrayList<>();

        StepVerifier.create(
                        service.boundedItemEvents(upstream.flux(), "feed-id")
                                .doOnNext(delivered::add),
                        0
                )
                .expectSubscription()
                .then(() -> {
                    for (int index = 0; index <= capacity; index++) {
                        upstream.next(itemEvent(index));
                    }
                })
                .thenRequest(capacity + 1L)
                .thenConsumeWhile(ignored -> true)
                .expectError(ItemStreamBackpressureException.class)
                .verify(Duration.ofSeconds(3));

        assertThat(delivered).hasSize(capacity);
        for (int index = 0; index < delivered.size(); index++) {
            ItemResponse item = (ItemResponse) delivered.get(index).data();
            assertThat(item.guid()).isEqualTo("item-" + index);
        }
    }

    @Test
    void mapsChangeStreamHistoryLostCode286() {
        assertHistoryLostMapping(new MongoException(
                286,
                "resume token fell out of the oplog"
        ));
    }

    @Test
    void mapsMongo8UnknownResumeTokenCommand() {
        assertHistoryLostMapping(mongoCommandError(
                280,
                "ChangeStreamFatalError",
                List.of("NonResumableChangeStreamError"),
                "cannot resume stream; the resume token was not found"
        ));
    }

    @Test
    void leavesOtherMongo280ErrorsUnmapped() {
        List<MongoCommandException> unrelatedErrors = List.of(
                mongoCommandError(
                        281,
                        "ChangeStreamFatalError",
                        List.of("NonResumableChangeStreamError"),
                        "cannot resume stream; the resume token was not found"
                ),
                mongoCommandError(
                        280,
                        "OtherCommandError",
                        List.of("NonResumableChangeStreamError"),
                        "cannot resume stream; the resume token was not found"
                ),
                mongoCommandError(
                        280,
                        "ChangeStreamFatalError",
                        List.of(),
                        "cannot resume stream; the resume token was not found"
                ),
                mongoCommandError(
                        280,
                        "ChangeStreamFatalError",
                        List.of("NonResumableChangeStreamError"),
                        "some other fatal change stream error"
                )
        );

        ItemStreamService service = newService();
        for (MongoCommandException source : unrelatedErrors) {
            StepVerifier.create(service.boundedItemEvents(
                            Flux.<ServerSentEvent<Object>>error(source),
                            "feed-id"
                    ))
                    .expectErrorSatisfies(error ->
                            assertThat(error).isSameAs(source)
                    )
                    .verify(Duration.ofSeconds(1));
        }
    }

    private void assertHistoryLostMapping(MongoException source) {
        StepVerifier.create(newService().boundedItemEvents(
                        Flux.<ServerSentEvent<Object>>error(source),
                        "feed-id"
                ))
                .expectErrorSatisfies(error -> {
                    assertThat(error)
                            .isInstanceOf(ChangeStreamHistoryLostException.class);
                    assertThat(error.getCause()).isSameAs(source);
                })
                .verify(Duration.ofSeconds(1));
    }

    private ItemStreamService newService() {
        return new ItemStreamService(
                mock(ReactiveMongoTemplate.class),
                new FeedStreamProperties()
        );
    }

    private MongoCommandException mongoCommandError(
            int code,
            String codeName,
            List<String> errorLabels,
            String message
    ) {
        BsonDocument response = new BsonDocument("ok", new BsonDouble(0))
                .append("code", new BsonInt32(code))
                .append("codeName", new BsonString(codeName))
                .append("errmsg", new BsonString(message));
        if (!errorLabels.isEmpty()) {
            response.put(
                    "errorLabels",
                    new BsonArray(errorLabels.stream()
                            .map(BsonString::new)
                            .toList())
            );
        }
        return new MongoCommandException(response, new ServerAddress());
    }

    private ServerSentEvent<Object> itemEvent(int index) {
        ItemResponse item = new ItemResponse(
                "id-" + index,
                "feed-id",
                "item-" + index,
                "Title " + index,
                "https://example.com/items/" + index,
                null,
                null,
                java.util.Map.of()
        );
        return ServerSentEvent.builder((Object) item)
                .id("token-" + index)
                .event("item")
                .build();
    }
}
