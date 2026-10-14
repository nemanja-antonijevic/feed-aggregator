package com.nantonijevic.feeds.polling;

import com.nantonijevic.feeds.repository.FeedRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;

public class FeedPollingCoordinator {

    private static final int FEED_CONCURRENCY = 4;

    private final FeedRepository feedRepository;
    private final FeedPollOperation pollOperation;
    private final Duration interval;
    private final Scheduler scheduler;

    public FeedPollingCoordinator(
            FeedRepository feedRepository,
            FeedPollOperation pollOperation,
            Duration interval,
            Scheduler scheduler
    ) {
        this.feedRepository = feedRepository;
        this.pollOperation = pollOperation;
        this.interval = interval;
        this.scheduler = scheduler;
    }

    public Flux<Long> cycles() {
        return Flux.interval(Duration.ZERO, interval, scheduler)
                .onBackpressureDrop()
                .concatMap(
                        tick -> runCycle().thenReturn(tick),
                        0
                );
    }

    private Mono<Void> runCycle() {
        return feedRepository.findAll()
                .flatMap(pollOperation::poll, FEED_CONCURRENCY)
                .then();
    }
}
