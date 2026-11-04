package com.nantonijevic.feeds.config;

import java.util.List;

import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.domain.Item;
import org.bson.Document;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import reactor.core.publisher.Mono;

@Configuration(proxyBeanMethods = false)
public class FeedIndexConfig {

    @Bean
    SmartInitializingSingleton feedIndexes(
            ReactiveMongoTemplate reactiveMongoTemplate
    ) {
        return () -> reactiveMongoTemplate
                .indexOps(Feed.class)
                .createIndex(
                        new Index()
                                .on("url", Sort.Direction.ASC)
                                .named("url_unique")
                                .unique()
                )
                .block();
    }

    @Bean
    SmartInitializingSingleton itemIndexes(
            ReactiveMongoTemplate reactiveMongoTemplate
    ) {
        return () -> configureItemCollection(reactiveMongoTemplate)
                .then(reactiveMongoTemplate
                        .indexOps(Item.class)
                        .createIndex(
                                new Index()
                                        .on("feedId", Sort.Direction.ASC)
                                        .on("guid", Sort.Direction.ASC)
                                        .named("feed_id_guid_unique")
                                        .unique()
                        ))
                .then(reactiveMongoTemplate
                        .indexOps(Item.class)
                        .createIndex(
                                new Index()
                                        .on(
                                                "feedId",
                                                Sort.Direction.ASC
                                        )
                                        .on(
                                                "publishedAt",
                                                Sort.Direction.ASC
                                        )
                                        .on(
                                                "fetchedAt",
                                                Sort.Direction.ASC
                                        )
                                        .named(
                                                "feed_id_published_at_fetched_at"
                                        )
                        ))
                .block();
    }

    private Mono<Void> configureItemCollection(
            ReactiveMongoTemplate reactiveMongoTemplate
    ) {
        String collection = reactiveMongoTemplate
                .getCollectionName(Item.class);
        Document validator = itemValidator();

        return reactiveMongoTemplate
                .collectionExists(collection)
                .flatMap(exists -> reactiveMongoTemplate.executeCommand(
                        exists
                                ? new Document("collMod", collection)
                                        .append("validator", validator)
                                        .append("validationLevel", "strict")
                                        .append("validationAction", "error")
                                : new Document("create", collection)
                                        .append("validator", validator)
                                        .append("validationLevel", "strict")
                                        .append("validationAction", "error")
                ))
                .then();
    }

    private Document itemValidator() {
        Document properties = new Document()
                .append(
                        "_id",
                        new Document("bsonType", "objectId")
                )
                .append(
                        "feedId",
                        new Document("bsonType", "objectId")
                )
                .append(
                        "guid",
                        new Document("bsonType", "string")
                                .append("minLength", 1)
                )
                .append(
                        "title",
                        new Document(
                                "bsonType",
                                List.of("string", "null")
                        )
                )
                .append(
                        "link",
                        new Document(
                                "bsonType",
                                List.of("string", "null")
                        )
                )
                .append(
                        "publishedAt",
                        new Document(
                                "bsonType",
                                List.of("date", "null")
                        )
                )
                .append(
                        "fetchedAt",
                        new Document("bsonType", "date")
                )
                .append(
                        "extras",
                        new Document("bsonType", "object")
                );

        return new Document(
                "$jsonSchema",
                new Document("bsonType", "object")
                        .append(
                                "required",
                                List.of("feedId", "guid", "fetchedAt")
                        )
                        .append("properties", properties)
        );
    }
}
