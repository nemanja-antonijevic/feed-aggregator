package com.nantonijevic.feeds.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.nantonijevic.feeds.TestcontainersConfiguration;
import com.nantonijevic.feeds.domain.Item;
import com.nantonijevic.feeds.repository.ItemRepository;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.FieldLookupPolicy;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.index.IndexField;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class DailyItemCountsAggregationIntegrationTest {

    private static final String DAILY_COUNTS_INDEX =
            "feed_id_published_at_fetched_at";

    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    private ItemRepository itemRepository;

    @Autowired
    private FeedService feedService;

    @BeforeEach
    void cleanItems() {
        StepVerifier.create(itemRepository.deleteAll())
                .verifyComplete();
    }

    @Test
    void createsCoveringIndexForDailyItemCounts() {
        StepVerifier.create(
                        reactiveMongoTemplate
                                .indexOps(Item.class)
                                .getIndexInfo()
                                .filter(index -> DAILY_COUNTS_INDEX.equals(
                                        index.getName()
                                ))
                                .single()
                )
                .assertNext(index -> {
                    assertThat(index.isUnique()).isFalse();
                    assertThat(index.getIndexFields())
                            .extracting(IndexField::getKey)
                            .containsExactly(
                                    "feedId",
                                    "publishedAt",
                                    "fetchedAt"
                            );
                })
                .verifyComplete();
    }

    @Test
    void dailyCountsAggregationUsesCoveringIndex() {
        String requestedFeedId = new ObjectId().toHexString();
        String otherFeedId = new ObjectId().toHexString();

        StepVerifier.create(itemRepository.saveAll(items(
                        requestedFeedId,
                        otherFeedId
                )))
                .expectNextCount(250)
                .verifyComplete();

        StepVerifier.create(explain(requestedFeedId))
                .assertNext(explain -> {
                    Document cursor = cursor(explain);
                    Document executionStats = cursor.get(
                            "executionStats",
                            Document.class
                    );
                    String winningPlan = cursor.get(
                            "queryPlanner",
                            Document.class
                    ).get("winningPlan", Document.class).toJson();

                    assertThat(winningPlan)
                            .contains("IXSCAN")
                            .contains(DAILY_COUNTS_INDEX)
                            .doesNotContain("COLLSCAN");
                    assertThat(executionStats.get(
                            "totalKeysExamined",
                            Number.class
                    ).longValue()).isEqualTo(200L);
                    assertThat(executionStats.get(
                            "totalDocsExamined",
                            Number.class
                    ).longValue()).isZero();
                })
                .verifyComplete();
    }

    private Mono<Document> explain(String feedId) {
        Aggregation aggregation =
                feedService.dailyItemCountsAggregation(feedId);
        QueryMapper queryMapper = new QueryMapper(
                reactiveMongoTemplate.getConverter()
        );
        TypeBasedAggregationOperationContext context =
                new TypeBasedAggregationOperationContext(
                        Item.class,
                        reactiveMongoTemplate.getConverter()
                                .getMappingContext(),
                        queryMapper,
                        FieldLookupPolicy.relaxed()
                );
        Document aggregate = new Document(
                "aggregate",
                reactiveMongoTemplate.getCollectionName(Item.class)
        )
                .append(
                        "pipeline",
                        aggregation.toPipeline(context)
                )
                .append("cursor", new Document());
        Document command = new Document("explain", aggregate)
                .append("verbosity", "executionStats");

        return reactiveMongoTemplate.executeCommand(command);
    }

    private Document cursor(Document explain) {
        return explain.getList("stages", Document.class)
                .stream()
                .map(stage -> stage.get("$cursor", Document.class))
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow();
    }

    private List<Item> items(
            String requestedFeedId,
            String otherFeedId
    ) {
        List<Item> items = new ArrayList<>();

        for (int index = 0; index < 200; index++) {
            Instant publishedAt = index % 2 == 0
                    ? Instant.parse("2026-11-01T08:00:00Z")
                    : null;

            items.add(item(
                    requestedFeedId,
                    "requested-" + index,
                    publishedAt
            ));
        }

        for (int index = 0; index < 50; index++) {
            items.add(item(
                    otherFeedId,
                    "other-" + index,
                    Instant.parse("2026-11-02T08:00:00Z")
            ));
        }

        return items;
    }

    private Item item(
            String feedId,
            String guid,
            Instant publishedAt
    ) {
        return new Item(
                null,
                feedId,
                guid,
                guid,
                "https://example.com/items/" + guid,
                publishedAt,
                Instant.parse("2026-11-03T08:00:00Z")
        );
    }
}
