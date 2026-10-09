package com.nantonijevic.feeds.repository;

import com.nantonijevic.feeds.domain.Item;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

public interface ItemRepository
        extends ReactiveMongoRepository<Item, String> {
}
