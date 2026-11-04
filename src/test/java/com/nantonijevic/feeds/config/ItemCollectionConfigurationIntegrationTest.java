package com.nantonijevic.feeds.config;

import java.time.Instant;
import java.util.Date;

import com.mongodb.MongoWriteException;
import com.nantonijevic.feeds.TestcontainersConfiguration;
import com.nantonijevic.feeds.domain.Item;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class ItemCollectionConfigurationIntegrationTest {

    @Autowired
    private FeedIndexConfig feedIndexConfig;

    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;

    @Test
    void configuresValidatorOnExistingCollectionBeforeIndexes() {
        StepVerifier.create(
                        reactiveMongoTemplate
                                .dropCollection(Item.class)
                                .then(reactiveMongoTemplate
                                        .createCollection(Item.class))
                )
                .expectNextCount(1)
                .verifyComplete();

        SmartInitializingSingleton initializer =
                feedIndexConfig.itemIndexes(reactiveMongoTemplate);
        initializer.afterSingletonsInstantiated();

        Document invalidItem = new Document()
                .append("feedId", new ObjectId())
                .append("guid", 42)
                .append("fetchedAt", Date.from(Instant.now()));
        Mono<Void> insert = reactiveMongoTemplate
                .getCollection("items")
                .flatMap(collection -> Mono.from(
                        collection.insertOne(invalidItem)
                ))
                .then();

        StepVerifier.create(insert)
                .expectErrorSatisfies(error -> {
                    assertThat(error)
                            .isInstanceOf(MongoWriteException.class);
                    assertThat(((MongoWriteException) error)
                            .getError()
                            .getCode()).isEqualTo(121);
                })
                .verify();
    }
}
