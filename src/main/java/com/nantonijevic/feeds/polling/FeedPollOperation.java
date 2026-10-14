package com.nantonijevic.feeds.polling;

import com.nantonijevic.feeds.domain.Feed;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface FeedPollOperation {

    Mono<Void> poll(Feed feed);
}
