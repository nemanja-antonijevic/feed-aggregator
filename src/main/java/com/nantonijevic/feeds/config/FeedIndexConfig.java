package com.nantonijevic.feeds.config;

import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.domain.Item;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

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
        return () -> reactiveMongoTemplate
                .indexOps(Item.class)
                .createIndex(
                        new Index()
                                .on("feedId", Sort.Direction.ASC)
                                .on("guid", Sort.Direction.ASC)
                                .named("feed_id_guid_unique")
                                .unique()
                )
                .block();
    }
}
