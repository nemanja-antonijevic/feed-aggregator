package com.nantonijevic.feeds.testing;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

class BlockHoundCanaryTest {

    @Test
    void detectsThreadSleepOnParallelScheduler() {
        Mono<Integer> blockingTask = Mono.fromCallable(() -> {
            Thread.sleep(10);
            return 1;
        }).subscribeOn(Schedulers.parallel());

        StepVerifier.create(blockingTask)
                .expectError(BlockingOperationError.class)
                .verify(Duration.ofSeconds(2));
    }
}
