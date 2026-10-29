package com.nantonijevic.feeds.polling;

import com.nantonijevic.feeds.repository.FeedRepository;
import com.nantonijevic.feeds.service.FetchFeedService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
        prefix = "feeds.polling",
        name = "enabled",
        havingValue = "true"
)
@EnableConfigurationProperties(FeedPollingProperties.class)
public class FeedPollingConfiguration {

    @Bean(destroyMethod = "dispose")
    Scheduler feedPollingScheduler() {
        return Schedulers.newSingle("feed-polling", true);
    }

    @Bean
    FeedPollOperation feedPollOperation(
            FetchFeedService fetchFeedService
    ) {
        return new FetchFeedPollOperation(fetchFeedService);
    }

    @Bean
    FeedPollingCoordinator feedPollingCoordinator(
            FeedRepository feedRepository,
            FeedPollOperation feedPollOperation,
            FeedPollStatusStore statusStore,
            FeedPollingProperties properties,
            @Qualifier("feedPollingScheduler")
            Scheduler feedPollingScheduler,
            Clock clock
    ) {
        return new FeedPollingCoordinator(
                feedRepository,
                feedPollOperation,
                statusStore,
                properties.getInterval(),
                feedPollingScheduler,
                clock
        );
    }

    @Bean
    FeedPollingLifecycle feedPollingLifecycle(
            FeedPollingCoordinator coordinator
    ) {
        return new FeedPollingLifecycle(coordinator);
    }
}
