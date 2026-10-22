package com.nantonijevic.feeds.polling;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.repository.FeedRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

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

            return Mono.delay(Duration.ofSeconds(15), scheduler)
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
                .thenAwait(Duration.ofSeconds(15))
                .expectNext(0L)
                .then(() -> assertThat(cycleStartSeconds)
                        .containsExactly(0L))
                .thenAwait(Duration.ofSeconds(5))
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
}
