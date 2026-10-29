package com.nantonijevic.feeds.polling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.SmartLifecycle;
import reactor.core.Disposable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class FeedPollingLifecycle implements SmartLifecycle {

    public static final int PHASE =
            WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE
                    - 100;

    private static final Logger log =
            LoggerFactory.getLogger(FeedPollingLifecycle.class);

    private final FeedPollingCoordinator coordinator;
    private final AtomicReference<SubscriptionState> active =
            new AtomicReference<>();

    public FeedPollingLifecycle(
            FeedPollingCoordinator coordinator
    ) {
        this.coordinator = coordinator;
    }

    @Override
    public void start() {
        SubscriptionState candidate = new SubscriptionState();
        if (!active.compareAndSet(null, candidate)) {
            return;
        }

        try {
            Disposable disposable = coordinator.cycles().subscribe(
                    ignored -> {
                    },
                    error -> terminate(candidate, error),
                    () -> complete(candidate)
            );
            candidate.attach(disposable);
        } catch (RuntimeException error) {
            active.compareAndSet(candidate, null);
            candidate.dispose();
            throw error;
        }
    }

    @Override
    public void stop() {
        SubscriptionState current = active.getAndSet(null);
        if (current != null) {
            current.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        SubscriptionState current = active.get();
        return current != null && !current.isDisposed();
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    private void terminate(
            SubscriptionState owner,
            Throwable error
    ) {
        log.error(
                "event=polling_stream_terminated errorType={}",
                error.getClass().getSimpleName()
        );
        complete(owner);
    }

    private void complete(SubscriptionState owner) {
        active.compareAndSet(owner, null);
        owner.dispose();
    }

    private static final class SubscriptionState {

        private final AtomicReference<Disposable> disposable =
                new AtomicReference<>();
        private final AtomicBoolean disposeRequested =
                new AtomicBoolean();

        private void attach(Disposable value) {
            if (!disposable.compareAndSet(null, value)) {
                value.dispose();
                throw new IllegalStateException(
                        "polling subscription already attached"
                );
            }
            if (disposeRequested.get()) {
                value.dispose();
            }
        }

        private void dispose() {
            disposeRequested.set(true);
            Disposable current = disposable.get();
            if (current != null) {
                current.dispose();
            }
        }

        private boolean isDisposed() {
            Disposable current = disposable.get();
            return disposeRequested.get()
                    || (current != null && current.isDisposed());
        }
    }
}
