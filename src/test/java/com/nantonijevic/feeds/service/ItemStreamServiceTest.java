package com.nantonijevic.feeds.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.nantonijevic.feeds.config.FeedStreamProperties;
import com.nantonijevic.feeds.dto.ItemResponse;
import com.nantonijevic.feeds.exception.ItemStreamBackpressureException;
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
