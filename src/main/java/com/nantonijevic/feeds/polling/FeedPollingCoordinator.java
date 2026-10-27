package com.nantonijevic.feeds.polling;

import com.nantonijevic.feeds.exception.FeedSourceConnectionException;
import com.nantonijevic.feeds.exception.FeedSourceHttpException;
import com.nantonijevic.feeds.exception.FeedSourceTimeoutException;
import com.nantonijevic.feeds.repository.FeedRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;

public class FeedPollingCoordinator {

    private static final Logger log =
            LoggerFactory.getLogger(FeedPollingCoordinator.class);

    private static final int FEED_CONCURRENCY = 4;

    private static final Duration CATALOG_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration FEED_TIMEOUT = Duration.ofSeconds(12);
    private static final Duration RETRY_MIN_BACKOFF =
            Duration.ofMillis(500);
    private static final Duration RETRY_MAX_BACKOFF =
            Duration.ofMillis(750);
    private static final long MAX_RETRIES = 2;
    private static final double RETRY_MULTIPLIER = 2.0;

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
                        tick -> runCycle()
                                .thenReturn(tick)
                                .doOnError(error -> log.error(
                                        "event=poll_cycle_failed errorType={}",
                                        error.getClass().getSimpleName()
                                ))
                                .onErrorResume(error -> Mono.empty()),
                        0
                );
    }

    private Mono<Void> runCycle() {
        return feedRepository.findAll()
                .collectList()
                .timeout(CATALOG_TIMEOUT, scheduler)
                .flatMapMany(Flux::fromIterable)
                .flatMap(
                        feed -> Mono.defer(() -> pollOperation.poll(feed))
                                .retryWhen(retrySpec())
                                .timeout(FEED_TIMEOUT, scheduler)
                                .doOnError(error -> log.warn(
                                        "event=poll_feed_failed feedId={} "
                                                + "errorType={}",
                                        feed.getId(),
                                        error.getClass().getSimpleName()
                                ))
                                .onErrorResume(error -> Mono.empty()),
                        FEED_CONCURRENCY
                )
                .then();
    }

    private RetryBackoffSpec retrySpec() {
        return Retry.backoff(MAX_RETRIES, RETRY_MIN_BACKOFF)
                .maxBackoff(RETRY_MAX_BACKOFF)
                .multiplier(RETRY_MULTIPLIER)
                .jitter(0)
                .scheduler(scheduler)
                .filter(this::isRetryable)
                .onRetryExhaustedThrow(
                        (spec, signal) -> signal.failure()
                );
    }

    private boolean isRetryable(Throwable error) {
        return error instanceof FeedSourceTimeoutException
                || error instanceof FeedSourceConnectionException
                || (error instanceof FeedSourceHttpException httpError
                && httpError.isServerError());
    }
}
