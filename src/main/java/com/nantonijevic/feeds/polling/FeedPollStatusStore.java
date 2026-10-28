package com.nantonijevic.feeds.polling;

import java.time.Instant;

import reactor.core.publisher.Mono;

public interface FeedPollStatusStore {

    Mono<Void> recordSuccess(
            String feedId,
            Instant lastAttemptAt,
            Instant lastSuccessAt
    );

    Mono<Void> recordFailure(
            String feedId,
            Instant lastAttemptAt,
            String errorType
    );
}
