package com.nantonijevic.feeds.repository;

import com.nantonijevic.feeds.domain.Feed;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

public interface FeedRepository extends ReactiveMongoRepository<Feed, String> {
}
