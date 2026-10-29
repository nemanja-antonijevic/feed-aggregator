package com.nantonijevic.feeds;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;

import com.nantonijevic.feeds.polling.FeedPollOperation;
import com.nantonijevic.feeds.polling.FeedPollingCoordinator;
import com.nantonijevic.feeds.polling.FeedPollingLifecycle;
import reactor.core.scheduler.Scheduler;

import static org.assertj.core.api.Assertions.assertThat;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class FeedAggregatorApplicationTests {

    private final ApplicationContext applicationContext;

    @Autowired
    FeedAggregatorApplicationTests(
            ApplicationContext applicationContext
    ) {
        this.applicationContext = applicationContext;
    }

    @Test
    void contextLoads() {
        assertThat(applicationContext.getBeansOfType(
                FeedPollOperation.class
        )).isEmpty();
        assertThat(applicationContext.getBeansOfType(
                FeedPollingCoordinator.class
        )).isEmpty();
        assertThat(applicationContext.getBeansOfType(
                FeedPollingLifecycle.class
        )).isEmpty();
        assertThat(applicationContext.getBeansOfType(
                Scheduler.class
        )).isEmpty();
    }

}
