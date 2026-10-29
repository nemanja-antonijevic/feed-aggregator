package com.nantonijevic.feeds.polling;

import com.nantonijevic.feeds.dto.FetchFeedResponse;
import com.nantonijevic.feeds.repository.FeedRepository;
import com.nantonijevic.feeds.service.FetchFeedService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class FeedPollingConfigurationTest {

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner()
                    .withUserConfiguration(
                            FeedPollingConfiguration.class,
                            Dependencies.class
                    );

    @Test
    void pollingBeansAreAbsentByDefault() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(FeedPollOperation.class);
            assertThat(context).doesNotHaveBean(
                    FeedPollingCoordinator.class
            );
            assertThat(context).doesNotHaveBean(
                    FeedPollingLifecycle.class
            );
            assertThat(context).doesNotHaveBean(Scheduler.class);
            assertThat(context).doesNotHaveBean(
                    FeedPollingProperties.class
            );
        });
    }

    @Test
    void pollingBeansAreAbsentWhenExplicitlyDisabled() {
        contextRunner
                .withPropertyValues("feeds.polling.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(
                            FeedPollingLifecycle.class
                    );
                });
    }

    @Test
    void invalidIntervalIsIgnoredWhileDisabled() {
        contextRunner
                .withPropertyValues(
                        "feeds.polling.enabled=false",
                        "feeds.polling.interval=0ms"
                )
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void enabledPollingCreatesAllInfrastructureWithDefaults() {
        contextRunner
                .withPropertyValues("feeds.polling.enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(
                            FeedPollOperation.class
                    );
                    assertThat(context).hasSingleBean(
                            FeedPollingCoordinator.class
                    );
                    assertThat(context).hasSingleBean(
                            FeedPollingLifecycle.class
                    );
                    assertThat(context).hasSingleBean(Scheduler.class);
                    assertThat(context).hasSingleBean(
                            FeedPollingProperties.class
                    );
                    assertThat(context.getBean(
                            FeedPollingProperties.class
                    ).getInterval()).isEqualTo(Duration.ofMinutes(5));
                });
    }

    @Test
    void enabledPollingRejectsInvalidIntervalBeforeStartup() {
        contextRunner
                .withPropertyValues(
                        "feeds.polling.enabled=true",
                        "feeds.polling.interval=0ms"
                )
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(
                                    BindValidationException.class
                            );
                });
    }

    @Test
    void schedulerThreadIsNamedAndDaemon() {
        contextRunner
                .withPropertyValues("feeds.polling.enabled=true")
                .run(context -> {
                    Scheduler scheduler = context.getBean(Scheduler.class);
                    CountDownLatch scheduled = new CountDownLatch(1);
                    AtomicReference<String> threadName =
                            new AtomicReference<>();
                    AtomicBoolean daemon = new AtomicBoolean();

                    scheduler.schedule(() -> {
                        Thread thread = Thread.currentThread();
                        threadName.set(thread.getName());
                        daemon.set(thread.isDaemon());
                        scheduled.countDown();
                    });

                    assertThat(scheduled.await(1, TimeUnit.SECONDS))
                            .isTrue();
                    assertThat(threadName.get())
                            .startsWith("feed-polling-");
                    assertThat(daemon).isTrue();
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class Dependencies {

        @Bean
        FeedRepository feedRepository() {
            return (FeedRepository) Proxy.newProxyInstance(
                    FeedRepository.class.getClassLoader(),
                    new Class<?>[]{FeedRepository.class},
                    (proxy, method, arguments) -> {
                        if (method.getName().equals("findAll")) {
                            return Flux.empty();
                        }
                        if (method.getDeclaringClass().equals(Object.class)) {
                            return method.invoke(this, arguments);
                        }
                        throw new UnsupportedOperationException(
                                method.getName()
                        );
                    }
            );
        }

        @Bean
        FetchFeedService fetchFeedService() {
            return new FetchFeedService(
                    null,
                    null,
                    null,
                    null,
                    null
            ) {
                @Override
                public Mono<FetchFeedResponse> fetch(String feedId) {
                    return Mono.just(new FetchFeedResponse(0, 0, 0));
                }
            };
        }

        @Bean
        FeedPollStatusStore feedPollStatusStore() {
            return new FeedPollStatusStore() {
                @Override
                public Mono<Void> recordSuccess(
                        String feedId,
                        Instant lastAttemptAt,
                        Instant lastSuccessAt
                ) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> recordFailure(
                        String feedId,
                        Instant lastAttemptAt,
                        String errorType
                ) {
                    return Mono.empty();
                }
            };
        }

        @Bean
        Clock clock() {
            return Clock.systemUTC();
        }
    }
}
