package com.nantonijevic.feeds.polling;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.exception.FeedSourceBodyTooLargeException;
import com.nantonijevic.feeds.exception.FeedSourceConnectionException;
import com.nantonijevic.feeds.exception.FeedSourceHttpException;
import com.nantonijevic.feeds.exception.FeedSourceTimeoutException;
import com.nantonijevic.feeds.exception.InvalidFeedSourceException;
import com.nantonijevic.feeds.repository.FeedRepository;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FeedPollingCoordinatorTest {

    @Test
    void slowCycleDropsIntermediateTickAndStartsNextCycleOnRegularTick() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository feedRepository = mock(FeedRepository.class);

        Feed feed = new Feed(
                "507f1f77bcf86cd799439011",
                "https://example.com/feed.xml",
                "Example",
                Instant.parse("2026-10-14T08:00:00Z")
        );

        when(feedRepository.findAll()).thenReturn(Flux.just(feed));

        List<Long> cycleStartSeconds = new CopyOnWriteArrayList<>();

        FeedPollOperation pollOperation = ignored -> Mono.defer(() -> {
            cycleStartSeconds.add(scheduler.now(TimeUnit.SECONDS));

            return Mono.delay(Duration.ofSeconds(11), scheduler)
                    .then();
        });

        FeedPollingCoordinator coordinator = new FeedPollingCoordinator(
                feedRepository,
                pollOperation,
                Duration.ofSeconds(10),
                scheduler
        );

        StepVerifier.withVirtualTime(
                        coordinator::cycles,
                        () -> scheduler,
                        Long.MAX_VALUE
                )
                .thenAwait(Duration.ofSeconds(11))
                .expectNext(0L)
                .then(() -> assertThat(cycleStartSeconds)
                        .containsExactly(0L))
                .thenAwait(Duration.ofSeconds(9))
                .then(() -> assertThat(cycleStartSeconds)
                        .containsExactly(0L, 20L))
                .thenCancel()
                .verify();
    }

    @Test
    void catalogTimeoutDoesNotTerminatePollingStream() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository feedRepository = mock(FeedRepository.class);

        Feed feed = new Feed(
                "507f1f77bcf86cd799439011",
                "https://example.com/feed.xml",
                "Example",
                Instant.parse("2026-10-14T08:00:00Z")
        );

        when(feedRepository.findAll()).thenReturn(
                Flux.never(),
                Flux.just(feed)
        );

        List<Long> pollStartSeconds = new CopyOnWriteArrayList<>();

        FeedPollOperation pollOperation = ignored -> Mono.fromRunnable(
                () -> pollStartSeconds.add(
                        scheduler.now(TimeUnit.SECONDS)
                )
        );

        FeedPollingCoordinator coordinator = new FeedPollingCoordinator(
                feedRepository,
                pollOperation,
                Duration.ofSeconds(20),
                scheduler
        );

        StepVerifier.withVirtualTime(
                        coordinator::cycles,
                        () -> scheduler,
                        Long.MAX_VALUE
                )
                .thenAwait(Duration.ofSeconds(20))
                .expectNext(1L)
                .then(() -> assertThat(pollStartSeconds)
                        .containsExactly(20L))
                .thenCancel()
                .verify(Duration.ofSeconds(1));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("retryableErrors")
    void retriesAllowedFailureTwiceThenSucceeds(
            Supplier<RuntimeException> errorSupplier
    ) {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository feedRepository = mock(FeedRepository.class);
        Feed feed = feed("507f1f77bcf86cd799439011", "retry");

        when(feedRepository.findAll()).thenReturn(Flux.just(feed));

        AtomicInteger attempts = new AtomicInteger();
        FeedPollOperation pollOperation = ignored -> Mono.defer(() -> {
            if (attempts.incrementAndGet() < 3) {
                return Mono.error(errorSupplier.get());
            }

            return Mono.empty();
        });

        FeedPollingCoordinator coordinator = coordinator(
                feedRepository,
                pollOperation,
                scheduler
        );

        StepVerifier.withVirtualTime(
                        coordinator::cycles,
                        () -> scheduler,
                        Long.MAX_VALUE
                )
                .thenAwait(Duration.ofMillis(499))
                .then(() -> assertThat(attempts).hasValue(1))
                .thenAwait(Duration.ofMillis(1))
                .then(() -> assertThat(attempts).hasValue(2))
                .thenAwait(Duration.ofMillis(749))
                .then(() -> assertThat(attempts).hasValue(2))
                .thenAwait(Duration.ofMillis(1))
                .then(() -> assertThat(attempts).hasValue(3))
                .expectNext(0L)
                .thenCancel()
                .verify(Duration.ofSeconds(1));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nonRetryableErrors")
    void doesNotRetryFailureOutsideAllowlist(
            Supplier<RuntimeException> errorSupplier
    ) {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository feedRepository = mock(FeedRepository.class);
        Feed feed = feed("507f1f77bcf86cd799439011", "no-retry");

        when(feedRepository.findAll()).thenReturn(Flux.just(feed));

        AtomicInteger attempts = new AtomicInteger();
        FeedPollOperation pollOperation = ignored -> Mono.defer(() -> {
            attempts.incrementAndGet();
            return Mono.error(errorSupplier.get());
        });

        FeedPollingCoordinator coordinator = coordinator(
                feedRepository,
                pollOperation,
                scheduler
        );

        StepVerifier.withVirtualTime(
                        coordinator::cycles,
                        () -> scheduler,
                        Long.MAX_VALUE
                )
                .thenAwait(Duration.ofSeconds(2))
                .expectNext(0L)
                .thenCancel()
                .verify(Duration.ofSeconds(1));

        assertThat(attempts).hasValue(1);
    }

    @Test
    void exhaustedRetryLogsOriginalErrorAndDoesNotStopOtherFeed() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository feedRepository = mock(FeedRepository.class);
        Feed failingFeed = feed(
                "507f1f77bcf86cd799439011",
                "exhausted"
        );
        Feed successfulFeed = feed(
                "507f1f77bcf86cd799439012",
                "successful"
        );

        when(feedRepository.findAll()).thenReturn(
                Flux.just(failingFeed, successfulFeed)
        );

        AtomicInteger failedAttempts = new AtomicInteger();
        AtomicInteger successfulAttempts = new AtomicInteger();
        FeedPollOperation pollOperation = feed -> Mono.defer(() -> {
            if (feed.getId().equals(failingFeed.getId())) {
                failedAttempts.incrementAndGet();
                return Mono.error(new FeedSourceTimeoutException(
                        new TimeoutException("source timeout")
                ));
            }

            successfulAttempts.incrementAndGet();
            return Mono.empty();
        });

        FeedPollingCoordinator coordinator = coordinator(
                feedRepository,
                pollOperation,
                scheduler
        );

        Logger logger = pollingLogger();
        ListAppender<ILoggingEvent> appender = startAppender(logger);

        try {
            StepVerifier.withVirtualTime(
                            coordinator::cycles,
                            () -> scheduler,
                            Long.MAX_VALUE
                    )
                    .thenAwait(Duration.ofMillis(1250))
                    .expectNext(0L)
                    .thenCancel()
                    .verify(Duration.ofSeconds(1));

            assertThat(failedAttempts).hasValue(3);
            assertThat(successfulAttempts).hasValue(1);
            assertThat(appender.list)
                    .filteredOn(event -> event.getFormattedMessage()
                            .contains("event=poll_feed_failed"))
                    .singleElement()
                    .satisfies(event -> {
                        assertThat(event.getFormattedMessage()).isEqualTo(
                                "event=poll_feed_failed feedId="
                                        + failingFeed.getId()
                                        + " errorType="
                                        + "FeedSourceTimeoutException"
                        );
                        assertThat(event.getThrowableProxy()).isNull();
                    });
        } finally {
            stopAppender(logger, appender);
        }
    }

    @Test
    void feedBudgetTimeoutIsIsolatedAndLogged() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository feedRepository = mock(FeedRepository.class);
        Feed hangingFeed = feed(
                "507f1f77bcf86cd799439011",
                "hanging"
        );
        Feed successfulFeed = feed(
                "507f1f77bcf86cd799439012",
                "successful"
        );

        when(feedRepository.findAll()).thenReturn(
                Flux.just(hangingFeed, successfulFeed)
        );

        AtomicInteger successfulAttempts = new AtomicInteger();
        FeedPollOperation pollOperation = feed -> {
            if (feed.getId().equals(hangingFeed.getId())) {
                return Mono.never();
            }

            successfulAttempts.incrementAndGet();
            return Mono.empty();
        };

        FeedPollingCoordinator coordinator = coordinator(
                feedRepository,
                pollOperation,
                scheduler
        );

        Logger logger = pollingLogger();
        ListAppender<ILoggingEvent> appender = startAppender(logger);

        try {
            StepVerifier.withVirtualTime(
                            coordinator::cycles,
                            () -> scheduler,
                            Long.MAX_VALUE
                    )
                    .thenAwait(Duration.ofSeconds(12))
                    .expectNext(0L)
                    .thenCancel()
                    .verify(Duration.ofSeconds(1));

            assertThat(successfulAttempts).hasValue(1);
            assertThat(appender.list)
                    .filteredOn(event -> event.getFormattedMessage()
                            .contains("event=poll_feed_failed"))
                    .singleElement()
                    .satisfies(event -> assertThat(
                            event.getFormattedMessage()
                    ).isEqualTo(
                            "event=poll_feed_failed feedId="
                                    + hangingFeed.getId()
                                    + " errorType=TimeoutException"
                    ));
        } finally {
            stopAppender(logger, appender);
        }
    }

    @Test
    void feedBudgetCoversBackoffAndAllRetryAttemptsTogether() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository feedRepository = mock(FeedRepository.class);
        Feed feed = feed(
                "507f1f77bcf86cd799439011",
                "whole-budget"
        );

        when(feedRepository.findAll()).thenReturn(Flux.just(feed));

        AtomicInteger attempts = new AtomicInteger();
        FeedPollOperation pollOperation = ignored -> Mono.defer(() -> {
            if (attempts.incrementAndGet() == 1) {
                return Mono.error(new FeedSourceHttpException(503));
            }

            return Mono.never();
        });

        FeedPollingCoordinator coordinator = coordinator(
                feedRepository,
                pollOperation,
                scheduler
        );

        StepVerifier.withVirtualTime(
                        coordinator::cycles,
                        () -> scheduler,
                        Long.MAX_VALUE
                )
                .thenAwait(Duration.ofMillis(11_999))
                .then(() -> assertThat(attempts).hasValue(2))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(0L)
                .thenCancel()
                .verify(Duration.ofSeconds(1));
    }

    @Test
    void feedFailureDoesNotPreventOtherFeedsFromBeingPolled() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository feedRepository = mock(FeedRepository.class);

        Feed failingFeed = new Feed(
                "507f1f77bcf86cd799439011",
                "https://example.com/failing.xml",
                "Failing",
                Instant.parse("2026-10-22T08:00:00Z")
        );

        Feed successfulFeed = new Feed(
                "507f1f77bcf86cd799439012",
                "https://example.com/successful.xml",
                "Successful",
                Instant.parse("2026-10-22T08:00:00Z")
        );

        when(feedRepository.findAll()).thenReturn(
                Flux.just(failingFeed, successfulFeed)
        );

        List<String> attemptedFeedIds = new CopyOnWriteArrayList<>();

        FeedPollOperation pollOperation = feed -> Mono.defer(() -> {
            attemptedFeedIds.add(feed.getId());

            if (feed.getId().equals(failingFeed.getId())) {
                return Mono.error(new IllegalStateException("poll failed"));
            }

            return Mono.empty();
        });

        FeedPollingCoordinator coordinator = new FeedPollingCoordinator(
                feedRepository,
                pollOperation,
                Duration.ofHours(1),
                scheduler
        );

        StepVerifier.withVirtualTime(
                        coordinator::cycles,
                        () -> scheduler,
                        Long.MAX_VALUE
                )
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(0L)
                .thenCancel()
                .verify(Duration.ofSeconds(1));

        assertThat(attemptedFeedIds)
                .containsExactlyInAnyOrder(
                        failingFeed.getId(),
                        successfulFeed.getId()
                );
    }

    @Test
    void synchronousFeedFailureDoesNotPreventLaterFeedsFromBeingPolled() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository feedRepository = mock(FeedRepository.class);

        Feed firstFeed = new Feed(
                "507f1f77bcf86cd799439011",
                "https://example.com/first.xml",
                "First",
                Instant.parse("2026-10-22T08:00:00Z")
        );

        Feed failingFeed = new Feed(
                "507f1f77bcf86cd799439012",
                "https://example.com/failing.xml",
                "Failing",
                Instant.parse("2026-10-22T08:00:00Z")
        );

        Feed lastFeed = new Feed(
                "507f1f77bcf86cd799439013",
                "https://example.com/last.xml",
                "Last",
                Instant.parse("2026-10-22T08:00:00Z")
        );

        when(feedRepository.findAll()).thenReturn(
                Flux.just(firstFeed, failingFeed, lastFeed)
        );

        List<String> attemptedFeedIds = new CopyOnWriteArrayList<>();

        FeedPollOperation pollOperation = feed -> {
            attemptedFeedIds.add(feed.getId());

            if (feed.getId().equals(failingFeed.getId())) {
                throw new IllegalStateException("synchronous poll failure");
            }

            return Mono.empty();
        };

        FeedPollingCoordinator coordinator = new FeedPollingCoordinator(
                feedRepository,
                pollOperation,
                Duration.ofHours(1),
                scheduler
        );

        StepVerifier.withVirtualTime(
                        coordinator::cycles,
                        () -> scheduler,
                        Long.MAX_VALUE
                )
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(0L)
                .thenCancel()
                .verify(Duration.ofSeconds(1));

        assertThat(attemptedFeedIds)
                .containsExactlyInAnyOrder(
                        firstFeed.getId(),
                        failingFeed.getId(),
                        lastFeed.getId()
                );
    }

    @Test
    void feedFailureLogsSanitizedWarningAndCycleContinues() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository feedRepository = mock(FeedRepository.class);

        Feed failingFeed = new Feed(
                "507f1f77bcf86cd799439011",
                "https://user:secret@example.com/feed.xml?token=private",
                "Failing",
                Instant.parse("2026-10-22T08:00:00Z")
        );

        Feed successfulFeed = new Feed(
                "507f1f77bcf86cd799439012",
                "https://example.com/successful.xml",
                "Successful",
                Instant.parse("2026-10-22T08:00:00Z")
        );

        when(feedRepository.findAll()).thenReturn(
                Flux.just(failingFeed, successfulFeed)
        );

        FeedPollOperation pollOperation = feed -> {
            if (feed.getId().equals(failingFeed.getId())) {
                return Mono.error(new IllegalStateException(
                        "poll failed for " + failingFeed.getUrl()
                ));
            }

            return Mono.empty();
        };

        FeedPollingCoordinator coordinator = new FeedPollingCoordinator(
                feedRepository,
                pollOperation,
                Duration.ofHours(1),
                scheduler
        );

        Logger logger = (Logger) LoggerFactory.getLogger(
                FeedPollingCoordinator.class
        );

        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);

        try {
            StepVerifier.withVirtualTime(
                            coordinator::cycles,
                            () -> scheduler,
                            Long.MAX_VALUE
                    )
                    .thenAwait(Duration.ofSeconds(1))
                    .expectNext(0L)
                    .thenCancel()
                    .verify(Duration.ofSeconds(1));

            assertThat(appender.list)
                    .filteredOn(event -> event.getFormattedMessage()
                            .contains("event=poll_feed_failed"))
                    .singleElement()
                    .satisfies(event -> {
                        assertThat(event.getLevel()).isEqualTo(Level.WARN);
                        assertThat(event.getFormattedMessage()).isEqualTo(
                                "event=poll_feed_failed feedId="
                                        + failingFeed.getId()
                                        + " errorType=IllegalStateException"
                        );
                        assertThat(event.getFormattedMessage())
                                .doesNotContain(
                                        failingFeed.getUrl(),
                                        "user:secret",
                                        "secret",
                                        "token="
                                );
                        assertThat(event.getThrowableProxy()).isNull();
                    });
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void catalogFailureLogsSanitizedErrorAndPollingContinues() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository feedRepository = mock(FeedRepository.class);

        Feed feed = new Feed(
                "507f1f77bcf86cd799439011",
                "https://example.com/feed.xml",
                "Example",
                Instant.parse("2026-10-23T08:00:00Z")
        );

        when(feedRepository.findAll()).thenReturn(
                Flux.error(new IllegalStateException(
                        "catalog failed with secret data"
                )),
                Flux.just(feed)
        );

        FeedPollOperation pollOperation = ignored -> Mono.empty();

        FeedPollingCoordinator coordinator = new FeedPollingCoordinator(
                feedRepository,
                pollOperation,
                Duration.ofSeconds(20),
                scheduler
        );

        Logger logger = (Logger) LoggerFactory.getLogger(
                FeedPollingCoordinator.class
        );

        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);

        try {
            StepVerifier.withVirtualTime(
                            coordinator::cycles,
                            () -> scheduler,
                            Long.MAX_VALUE
                    )
                    .thenAwait(Duration.ofSeconds(20))
                    .expectNext(1L)
                    .thenCancel()
                    .verify(Duration.ofSeconds(1));

            assertThat(appender.list)
                    .filteredOn(event -> event.getFormattedMessage()
                            .contains("event=poll_cycle_failed"))
                    .singleElement()
                    .satisfies(event -> {
                        assertThat(event.getLevel()).isEqualTo(Level.ERROR);
                        assertThat(event.getFormattedMessage()).isEqualTo(
                                "event=poll_cycle_failed "
                                        + "errorType=IllegalStateException"
                        );
                        assertThat(event.getFormattedMessage())
                                .doesNotContain("secret data");
                        assertThat(event.getThrowableProxy()).isNull();
                    });
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void nullPollResultDoesNotPreventOtherFeedsFromBeingPolled() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository feedRepository = mock(FeedRepository.class);

        Feed nullFeed = new Feed(
                "507f1f77bcf86cd799439011",
                "https://example.com/null.xml",
                "Null",
                Instant.parse("2026-10-23T08:00:00Z")
        );

        Feed successfulFeed = new Feed(
                "507f1f77bcf86cd799439012",
                "https://example.com/successful.xml",
                "Successful",
                Instant.parse("2026-10-23T08:00:00Z")
        );

        when(feedRepository.findAll()).thenReturn(
                Flux.just(nullFeed, successfulFeed)
        );

        List<String> attemptedFeedIds = new CopyOnWriteArrayList<>();

        FeedPollOperation pollOperation = feed -> {
            attemptedFeedIds.add(feed.getId());

            if (feed.getId().equals(nullFeed.getId())) {
                return null;
            }

            return Mono.empty();
        };

        FeedPollingCoordinator coordinator = new FeedPollingCoordinator(
                feedRepository,
                pollOperation,
                Duration.ofHours(1),
                scheduler
        );

        Logger logger = (Logger) LoggerFactory.getLogger(
                FeedPollingCoordinator.class
        );

        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);

        try {
            StepVerifier.withVirtualTime(
                            coordinator::cycles,
                            () -> scheduler,
                            Long.MAX_VALUE
                    )
                    .thenAwait(Duration.ofSeconds(1))
                    .expectNext(0L)
                    .thenCancel()
                    .verify(Duration.ofSeconds(1));

            assertThat(attemptedFeedIds)
                    .containsExactlyInAnyOrder(
                            nullFeed.getId(),
                            successfulFeed.getId()
                    );

            assertThat(appender.list)
                    .filteredOn(event -> event.getFormattedMessage()
                            .contains("event=poll_feed_failed"))
                    .singleElement()
                    .satisfies(event -> {
                        assertThat(event.getLevel()).isEqualTo(Level.WARN);
                        assertThat(event.getFormattedMessage()).isEqualTo(
                                "event=poll_feed_failed feedId="
                                        + nullFeed.getId()
                                        + " errorType=NullPointerException"
                        );
                        assertThat(event.getThrowableProxy()).isNull();
                    });

            assertThat(appender.list)
                    .noneMatch(event -> event.getFormattedMessage()
                            .contains("event=poll_cycle_failed"));
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    private static Stream<Arguments> retryableErrors() {
        return Stream.of(
                Arguments.of(Named.of(
                        "timeout",
                        (Supplier<RuntimeException>) () ->
                                new FeedSourceTimeoutException(
                                        new TimeoutException(
                                                "source timeout"
                                        )
                                )
                )),
                Arguments.of(Named.of(
                        "HTTP 503",
                        (Supplier<RuntimeException>) () ->
                                new FeedSourceHttpException(503)
                )),
                Arguments.of(Named.of(
                        "connection failure",
                        (Supplier<RuntimeException>) () ->
                                new FeedSourceConnectionException(
                                        new IllegalStateException(
                                                "connection closed"
                                        )
                                )
                ))
        );
    }

    private static Stream<Arguments> nonRetryableErrors() {
        return Stream.of(
                Arguments.of(Named.of(
                        "HTTP 404",
                        (Supplier<RuntimeException>) () ->
                                new FeedSourceHttpException(404)
                )),
                Arguments.of(Named.of(
                        "oversized body",
                        (Supplier<RuntimeException>) () ->
                                new FeedSourceBodyTooLargeException(
                                        new IllegalStateException(
                                                "body too large"
                                        )
                                )
                )),
                Arguments.of(Named.of(
                        "parser failure",
                        (Supplier<RuntimeException>) () ->
                                new InvalidFeedSourceException(
                                        "invalid feed"
                                )
                )),
                Arguments.of(Named.of(
                        "invalid URL",
                        (Supplier<RuntimeException>) () ->
                                new IllegalArgumentException(
                                        "invalid URL"
                                )
                )),
                Arguments.of(Named.of(
                        "unknown failure",
                        (Supplier<RuntimeException>) () ->
                                new IllegalStateException(
                                        "unknown failure"
                                )
                ))
        );
    }

    private Feed feed(String id, String name) {
        return new Feed(
                id,
                "https://example.com/" + name + ".xml",
                name,
                Instant.parse("2026-10-27T08:00:00Z")
        );
    }

    private FeedPollingCoordinator coordinator(
            FeedRepository feedRepository,
            FeedPollOperation pollOperation,
            VirtualTimeScheduler scheduler
    ) {
        return new FeedPollingCoordinator(
                feedRepository,
                pollOperation,
                Duration.ofHours(1),
                scheduler
        );
    }

    private Logger pollingLogger() {
        return (Logger) LoggerFactory.getLogger(
                FeedPollingCoordinator.class
        );
    }

    private ListAppender<ILoggingEvent> startAppender(Logger logger) {
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        return appender;
    }

    private void stopAppender(
            Logger logger,
            ListAppender<ILoggingEvent> appender
    ) {
        logger.detachAppender(appender);
        appender.stop();
    }
}
