package com.nantonijevic.feeds.polling;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.publisher.TestPublisher;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class FeedPollingLifecycleTest {

    @Test
    void repeatedStartCreatesOnlyOneSubscription() {
        AtomicInteger subscriptions = new AtomicInteger();
        FeedPollingLifecycle lifecycle = lifecycle(() -> {
            subscriptions.incrementAndGet();
            return Flux.never();
        });

        lifecycle.start();
        lifecycle.start();

        assertThat(lifecycle.isRunning()).isTrue();
        assertThat(subscriptions).hasValue(1);

        lifecycle.stop();
        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    void stopCancelsTheActiveSubscriptionImmediately() {
        AtomicInteger cancellations = new AtomicInteger();
        FeedPollingLifecycle lifecycle = lifecycle(() ->
                Flux.<Long>never()
                        .doOnCancel(cancellations::incrementAndGet)
        );

        lifecycle.start();
        lifecycle.stop();

        assertThat(cancellations).hasValue(1);
        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    void stopThenStartUsesTheSameLiveScheduler() throws Exception {
        Scheduler scheduler = Schedulers.newSingle(
                "lifecycle-restart-test",
                true
        );
        CountDownLatch firstTick = new CountDownLatch(1);
        CountDownLatch secondTick = new CountDownLatch(1);
        AtomicInteger subscriptions = new AtomicInteger();

        FeedPollingLifecycle lifecycle = lifecycle(() -> {
            int subscription = subscriptions.incrementAndGet();
            return Flux.interval(
                            Duration.ZERO,
                            Duration.ofDays(1),
                            scheduler
                    )
                    .doOnNext(ignored -> {
                        if (subscription == 1) {
                            firstTick.countDown();
                        } else {
                            secondTick.countDown();
                        }
                    });
        });

        try {
            lifecycle.start();
            assertThat(firstTick.await(1, TimeUnit.SECONDS)).isTrue();

            lifecycle.stop();
            lifecycle.start();

            assertThat(secondTick.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(subscriptions).hasValue(2);
            assertThat(lifecycle.isRunning()).isTrue();
        } finally {
            lifecycle.stop();
            scheduler.dispose();
        }
    }

    @Test
    void terminalErrorIsSanitizedAndClearsRunningState() {
        AtomicInteger subscriptions = new AtomicInteger();
        FeedPollingLifecycle lifecycle = lifecycle(() -> {
            subscriptions.incrementAndGet();
            return Flux.error(new IllegalStateException("secret URL"));
        });
        Logger logger = logger();
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);

        try {
            lifecycle.start();

            assertThat(lifecycle.isRunning()).isFalse();
            assertThat(subscriptions).hasValue(1);
            assertThat(appender.list)
                    .filteredOn(event -> event.getFormattedMessage()
                            .contains("event=polling_stream_terminated"))
                    .singleElement()
                    .satisfies(event -> {
                        assertThat(event.getLevel()).isEqualTo(Level.ERROR);
                        assertThat(event.getFormattedMessage()).isEqualTo(
                                "event=polling_stream_terminated "
                                        + "errorType=IllegalStateException"
                        );
                        assertThat(event.getFormattedMessage())
                                .doesNotContain("secret URL");
                        assertThat(event.getThrowableProxy()).isNull();
                    });
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void defaultStopCallbackRunsInEveryLifecycleState() {
        AtomicInteger callbacks = new AtomicInteger();

        FeedPollingLifecycle neverStarted = lifecycle(Flux::never);
        neverStarted.stop(callbacks::incrementAndGet);

        FeedPollingLifecycle running = lifecycle(Flux::never);
        running.start();
        running.stop(callbacks::incrementAndGet);

        FeedPollingLifecycle terminated = lifecycle(() ->
                Flux.error(new IllegalStateException("failed"))
        );
        terminated.start();
        terminated.stop(callbacks::incrementAndGet);

        assertThat(callbacks).hasValue(3);
    }

    @Test
    void lateTerminalCallbackCannotClearRestartedSubscription()
            throws Exception {
        TestPublisher<Long> first = TestPublisher.create();
        AtomicInteger subscriptions = new AtomicInteger();
        FeedPollingLifecycle lifecycle = lifecycle(() ->
                subscriptions.incrementAndGet() == 1
                        ? first.flux()
                        : Flux.never()
        );

        CountDownLatch terminalLogEntered = new CountDownLatch(1);
        CountDownLatch allowTerminalCallback = new CountDownLatch(1);
        Logger logger = logger();
        ListAppender<ILoggingEvent> blockingAppender =
                new ListAppender<>() {
                    @Override
                    protected void append(ILoggingEvent event) {
                        if (event.getFormattedMessage().contains(
                                "event=polling_stream_terminated"
                        )) {
                            terminalLogEntered.countDown();
                            await(allowTerminalCallback);
                        }
                    }
                };
        blockingAppender.start();
        logger.addAppender(blockingAppender);

        Thread terminalThread = new Thread(
                () -> first.error(new IllegalStateException("failed")),
                "old-polling-terminal"
        );

        try {
            lifecycle.start();
            terminalThread.start();
            assertThat(terminalLogEntered.await(1, TimeUnit.SECONDS))
                    .isTrue();

            lifecycle.stop();
            lifecycle.start();
            allowTerminalCallback.countDown();
            terminalThread.join(1_000);

            assertThat(terminalThread.isAlive()).isFalse();
            assertThat(subscriptions).hasValue(2);
            assertThat(lifecycle.isRunning()).isTrue();
        } finally {
            allowTerminalCallback.countDown();
            lifecycle.stop();
            logger.detachAppender(blockingAppender);
            blockingAppender.stop();
        }
    }

    @Test
    void phaseIsExplicit() {
        assertThat(lifecycle(Flux::never).getPhase())
                .isEqualTo(
                        WebServerApplicationContext
                                .GRACEFUL_SHUTDOWN_PHASE - 100
                );
    }

    private FeedPollingLifecycle lifecycle(
            Supplier<Flux<Long>> cycles
    ) {
        return new FeedPollingLifecycle(
                new StubCoordinator(cycles)
        );
    }

    private Logger logger() {
        return (Logger) LoggerFactory.getLogger(
                FeedPollingLifecycle.class
        );
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(error);
        }
    }

    private static final class StubCoordinator
            extends FeedPollingCoordinator {

        private final Supplier<Flux<Long>> cycles;

        private StubCoordinator(Supplier<Flux<Long>> cycles) {
            super(
                    null,
                    null,
                    null,
                    Duration.ofMinutes(5),
                    Schedulers.immediate(),
                    Clock.systemUTC()
            );
            this.cycles = cycles;
        }

        @Override
        public Flux<Long> cycles() {
            return cycles.get();
        }
    }
}
