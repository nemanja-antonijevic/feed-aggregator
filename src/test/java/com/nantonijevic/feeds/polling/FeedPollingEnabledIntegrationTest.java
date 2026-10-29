package com.nantonijevic.feeds.polling;

import com.nantonijevic.feeds.TestcontainersConfiguration;
import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.domain.Item;
import com.nantonijevic.feeds.repository.FeedRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "feeds.polling.enabled=true",
                "feeds.polling.interval=1h"
        }
)
@Import({
        TestcontainersConfiguration.class,
        FeedPollingEnabledIntegrationTest.PollProbeConfig.class
})
class FeedPollingEnabledIntegrationTest {

    private final ApplicationContext applicationContext;
    private final FeedPollingLifecycle pollingLifecycle;
    private final IndexCheckingPollOperation pollOperation;

    @Autowired
    FeedPollingEnabledIntegrationTest(
            ApplicationContext applicationContext,
            FeedPollingLifecycle pollingLifecycle,
            IndexCheckingPollOperation pollOperation
    ) {
        this.applicationContext = applicationContext;
        this.pollingLifecycle = pollingLifecycle;
        this.pollOperation = pollOperation;
    }

    @Test
    void indexesExistBeforeFirstProductionPoll() throws Exception {
        assertThat(pollOperation.polled.await(2, TimeUnit.SECONDS))
                .isTrue();
        assertThat(pollOperation.indexesPresent).isTrue();
    }

    @Test
    void pollingPhaseIsBetweenGracefulAndStartStopWebPhases() {
        SmartLifecycle gracefulShutdown = webLifecycle(
                "WebServerGracefulShutdownLifecycle"
        );
        SmartLifecycle startStop = webLifecycle(
                "WebServerStartStopLifecycle"
        );

        assertThat(pollingLifecycle.getPhase())
                .isLessThan(gracefulShutdown.getPhase())
                .isGreaterThan(startStop.getPhase());
    }

    private SmartLifecycle webLifecycle(String simpleName) {
        return applicationContext
                .getBeansOfType(SmartLifecycle.class)
                .values()
                .stream()
                .filter(bean -> bean.getClass().getName()
                        .contains(simpleName))
                .findFirst()
                .orElseThrow();
    }

    @Test
    void pollingRestartsOnTheProductionScheduler() {
        pollingLifecycle.stop();
        int pollsBeforeRestart = pollOperation.startedPolls.get();

        pollingLifecycle.start();

        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(pollOperation.startedPolls.get())
                        .isGreaterThan(pollsBeforeRestart)
        );
        assertThat(pollingLifecycle.isRunning()).isTrue();
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class PollProbeConfig {

        @Bean
        @Primary
        IndexCheckingPollOperation indexCheckingPollOperation(
                ReactiveMongoTemplate mongoTemplate
        ) {
            return new IndexCheckingPollOperation(mongoTemplate);
        }

        @Bean
        @DependsOn({"feedIndexes", "itemIndexes"})
        SmartInitializingSingleton pollingTestFeed(
                FeedRepository feedRepository
        ) {
            return () -> feedRepository.save(new Feed(
                    null,
                    "https://example.com/lifecycle.xml",
                    "Lifecycle",
                    Instant.parse("2026-10-29T08:00:00Z")
            )).block();
        }
    }

    static class IndexCheckingPollOperation
            implements FeedPollOperation {

        private final ReactiveMongoTemplate mongoTemplate;
        private final CountDownLatch polled = new CountDownLatch(1);
        private final AtomicInteger startedPolls = new AtomicInteger();
        private volatile boolean indexesPresent;

        IndexCheckingPollOperation(
                ReactiveMongoTemplate mongoTemplate
        ) {
            this.mongoTemplate = mongoTemplate;
        }

        @Override
        public Mono<Void> poll(Feed feed) {
            startedPolls.incrementAndGet();
            return Mono.zip(
                            mongoTemplate.indexOps(Feed.class)
                                    .getIndexInfo()
                                    .map(index -> index.getName())
                                    .collectList(),
                            mongoTemplate.indexOps(Item.class)
                                    .getIndexInfo()
                                    .map(index -> index.getName())
                                    .collectList()
                    )
                    .doOnNext(indexes -> indexesPresent =
                            Set.copyOf(indexes.getT1())
                                    .contains("url_unique")
                                    && Set.copyOf(indexes.getT2())
                                    .contains("feed_id_guid_unique"))
                    .doFinally(ignored -> polled.countDown())
                    .then();
        }
    }
}
