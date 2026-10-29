package com.nantonijevic.feeds.polling;

import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.exception.FeedNotFoundException;
import com.nantonijevic.feeds.service.FetchFeedService;
import reactor.core.publisher.Mono;

public class FetchFeedPollOperation implements FeedPollOperation {

    private final FetchFeedService fetchFeedService;

    public FetchFeedPollOperation(
            FetchFeedService fetchFeedService
    ) {
        this.fetchFeedService = fetchFeedService;
    }

    @Override
    public Mono<Void> poll(Feed feed) {
        return fetchFeedService.fetch(feed.getId())
                .onErrorResume(
                        FeedNotFoundException.class,
                        error -> Mono.empty()
                )
                .then();
    }
}
