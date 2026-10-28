package com.nantonijevic.feeds.polling;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.exception.FeedSourceHttpException;
import com.nantonijevic.feeds.repository.FeedRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FeedPollingStatusCoordinatorTest {

    private static final Instant BASE_TIME =
            Instant.parse("2026-10-28T08:00:00Z");

    @Test
    void emptyCompletionRecordsSuccessfulPoll() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository repository = repositoryWith(feed("successful"));
        RecordingStatusStore statusStore = new RecordingStatusStore();

        FeedPollingCoordinator coordinator = coordinator(
                repository,
                ignored -> Mono.empty(),
                statusStore,
                scheduler
        );

        StepVerifier.withVirtualTime(
                        coordinator::cycles,
                        () -> scheduler,
                        Long.MAX_VALUE
                )
                .expectNext(0L)
                .thenCancel()
                .verify(Duration.ofSeconds(1));

        assertThat(statusStore.successes).singleElement()
                .satisfies(success -> {
                    assertThat(success.lastAttemptAt()).isEqualTo(BASE_TIME);
                    assertThat(success.lastSuccessAt()).isEqualTo(BASE_TIME);
                });
        assertThat(statusStore.failures).isEmpty();
    }

    @Test
    void retryPreservesAttemptStartAndRecordsLaterSuccess() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository repository = repositoryWith(feed("retry"));
        RecordingStatusStore statusStore = new RecordingStatusStore();
        AtomicInteger attempts = new AtomicInteger();

        FeedPollOperation operation = ignored -> Mono.defer(() -> {
            if (attempts.incrementAndGet() == 1) {
                return Mono.error(new FeedSourceHttpException(503));
            }
            return Mono.empty();
        });

        FeedPollingCoordinator coordinator = coordinator(
                repository,
                operation,
                statusStore,
                scheduler
        );

        StepVerifier.withVirtualTime(
                        coordinator::cycles,
                        () -> scheduler,
                        Long.MAX_VALUE
                )
                .thenAwait(Duration.ofMillis(500))
                .expectNext(0L)
                .thenCancel()
                .verify(Duration.ofSeconds(1));

        assertThat(attempts).hasValue(2);
        assertThat(statusStore.successes).singleElement()
                .satisfies(success -> {
                    assertThat(success.lastAttemptAt()).isEqualTo(BASE_TIME);
                    assertThat(success.lastSuccessAt()).isEqualTo(
                            BASE_TIME.plusMillis(500)
                    );
                });
    }

    @Test
    void wholeFeedTimeoutRecordsOriginalTimeoutFailure() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository repository = repositoryWith(feed("timeout"));
        RecordingStatusStore statusStore = new RecordingStatusStore();
        AtomicInteger attempts = new AtomicInteger();

        FeedPollOperation operation = ignored -> Mono.defer(() -> {
            if (attempts.incrementAndGet() == 1) {
                return Mono.error(new FeedSourceHttpException(503));
            }
            return Mono.never();
        });

        FeedPollingCoordinator coordinator = coordinator(
                repository,
                operation,
                statusStore,
                scheduler
        );

        StepVerifier.withVirtualTime(
                        coordinator::cycles,
                        () -> scheduler,
                        Long.MAX_VALUE
                )
                .thenAwait(Duration.ofSeconds(12))
                .expectNext(0L)
                .thenCancel()
                .verify(Duration.ofSeconds(1));

        assertThat(attempts).hasValue(2);
        assertThat(statusStore.successes).isEmpty();
        assertThat(statusStore.failures).singleElement()
                .satisfies(failure -> {
                    assertThat(failure.lastAttemptAt()).isEqualTo(BASE_TIME);
                    assertThat(failure.errorType())
                            .isEqualTo("TimeoutException");
                });
    }

    @Test
    void statusWriteFailureIsLoggedWithoutBecomingPollFailure() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository repository = repositoryWith(feed("status-failure"));
        RecordingStatusStore statusStore = new RecordingStatusStore();
        statusStore.successError = new IllegalStateException(
                "secret status failure"
        );

        FeedPollingCoordinator coordinator = coordinator(
                repository,
                ignored -> Mono.empty(),
                statusStore,
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
                    .expectNext(0L)
                    .thenCancel()
                    .verify(Duration.ofSeconds(1));

            assertThat(appender.list)
                    .filteredOn(event -> event.getFormattedMessage()
                            .contains("event=poll_status_write_failed"))
                    .singleElement()
                    .satisfies(event -> {
                        assertThat(event.getFormattedMessage()).isEqualTo(
                                "event=poll_status_write_failed feedId="
                                        + feed("status-failure").getId()
                                        + " errorType=IllegalStateException"
                        );
                        assertThat(event.getThrowableProxy()).isNull();
                    });
            assertThat(appender.list).noneMatch(event ->
                    event.getFormattedMessage()
                            .contains("event=poll_feed_failed")
            );
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void hangingStatusWriteTimesOutAndCycleCompletes() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository repository = repositoryWith(feed("status-timeout"));
        RecordingStatusStore statusStore = new RecordingStatusStore();
        statusStore.successNeverCount = 1;

        FeedPollingCoordinator coordinator = coordinator(
                repository,
                ignored -> Mono.empty(),
                statusStore,
                scheduler,
                Duration.ofSeconds(3)
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
                    .thenAwait(Duration.ofMillis(1_999))
                    .then(() -> assertThat(appender.list).noneMatch(event ->
                            event.getFormattedMessage().contains(
                                    "event=poll_status_write_failed"
                            )
                    ))
                    .thenAwait(Duration.ofMillis(1))
                    .expectNext(0L)
                    .thenAwait(Duration.ofSeconds(1))
                    .expectNext(1L)
                    .thenCancel()
                    .verify(Duration.ofSeconds(1));

            assertThat(appender.list)
                    .filteredOn(event -> event.getFormattedMessage()
                            .contains("event=poll_status_write_failed"))
                    .singleElement()
                    .satisfies(event -> assertThat(
                            event.getFormattedMessage()
                    ).isEqualTo(
                            "event=poll_status_write_failed feedId="
                                    + feed("status-timeout").getId()
                                    + " errorType=TimeoutException"
                    ));
            assertThat(appender.list).noneMatch(event ->
                    event.getFormattedMessage()
                            .contains("event=poll_feed_failed")
            );
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void failedPollAndFailedStatusWriteAreLoggedSeparatelyOnce() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        FeedRepository repository = repositoryWith(feed("double-failure"));
        RecordingStatusStore statusStore = new RecordingStatusStore();
        statusStore.failureError = new IllegalStateException(
                "secret status failure"
        );

        FeedPollingCoordinator coordinator = coordinator(
                repository,
                ignored -> Mono.error(
                        new IllegalArgumentException("poll failed")
                ),
                statusStore,
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
                    .expectNext(0L)
                    .thenCancel()
                    .verify(Duration.ofSeconds(1));

            assertThat(appender.list)
                    .filteredOn(event -> event.getFormattedMessage()
                            .contains("event=poll_feed_failed"))
                    .singleElement();
            assertThat(appender.list)
                    .filteredOn(event -> event.getFormattedMessage()
                            .contains("event=poll_status_write_failed"))
                    .singleElement();
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    private FeedPollingCoordinator coordinator(
            FeedRepository repository,
            FeedPollOperation operation,
            FeedPollStatusStore statusStore,
            VirtualTimeScheduler scheduler
    ) {
        return coordinator(
                repository,
                operation,
                statusStore,
                scheduler,
                Duration.ofHours(1)
        );
    }

    private FeedPollingCoordinator coordinator(
            FeedRepository repository,
            FeedPollOperation operation,
            FeedPollStatusStore statusStore,
            VirtualTimeScheduler scheduler,
            Duration interval
    ) {
        return new FeedPollingCoordinator(
                repository,
                operation,
                statusStore,
                interval,
                scheduler,
                new SchedulerClock(BASE_TIME, scheduler)
        );
    }

    private FeedRepository repositoryWith(Feed feed) {
        FeedRepository repository = mock(FeedRepository.class);
        when(repository.findAll()).thenReturn(Flux.just(feed));
        return repository;
    }

    private Feed feed(String name) {
        return new Feed(
                "507f1f77bcf86cd799439011",
                "https://example.com/" + name + ".xml",
                name,
                BASE_TIME
        );
    }

    private record Success(
            String feedId,
            Instant lastAttemptAt,
            Instant lastSuccessAt
    ) {
    }

    private record Failure(
            String feedId,
            Instant lastAttemptAt,
            String errorType
    ) {
    }

    private static class RecordingStatusStore
            implements FeedPollStatusStore {

        private final List<Success> successes =
                new CopyOnWriteArrayList<>();
        private final List<Failure> failures =
                new CopyOnWriteArrayList<>();
        private RuntimeException successError;
        private RuntimeException failureError;
        private int successNeverCount;

        @Override
        public Mono<Void> recordSuccess(
                String feedId,
                Instant lastAttemptAt,
                Instant lastSuccessAt
        ) {
            if (successNeverCount > 0) {
                successNeverCount--;
                return Mono.never();
            }
            if (successError != null) {
                return Mono.error(successError);
            }
            successes.add(new Success(
                    feedId,
                    lastAttemptAt,
                    lastSuccessAt
            ));
            return Mono.empty();
        }

        @Override
        public Mono<Void> recordFailure(
                String feedId,
                Instant lastAttemptAt,
                String errorType
        ) {
            if (failureError != null) {
                return Mono.error(failureError);
            }
            failures.add(new Failure(feedId, lastAttemptAt, errorType));
            return Mono.empty();
        }
    }

    private static class SchedulerClock extends Clock {

        private final Instant baseTime;
        private final Scheduler scheduler;

        private SchedulerClock(
                Instant baseTime,
                Scheduler scheduler
        ) {
            this.baseTime = baseTime;
            this.scheduler = scheduler;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            if (!ZoneOffset.UTC.equals(zone)) {
                throw new IllegalArgumentException("only UTC is supported");
            }
            return this;
        }

        @Override
        public Instant instant() {
            return baseTime.plusMillis(
                    scheduler.now(TimeUnit.MILLISECONDS)
            );
        }
    }
}
