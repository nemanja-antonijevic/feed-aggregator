package com.nantonijevic.feeds.polling;

import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.repository.FeedRepository;
import org.junit.jupiter.api.Test;
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
}
