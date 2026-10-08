package com.nantonijevic.feeds.service;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.exception.FeedNotFoundException;
import com.nantonijevic.feeds.exception.InvalidFeedIdException;
import com.nantonijevic.feeds.repository.FeedRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FeedServiceTest {

    private static final Instant NOW =
            Instant.parse("2026-10-07T07:00:00.123456789Z");

    private static final Instant STORED_TIME =
            Instant.parse("2026-10-07T07:00:00.123Z");

    private static final String ID =
            "507f1f77bcf86cd799439011";

    @Mock
    private FeedRepository feedRepository;

    @Mock
    private ReactiveMongoTemplate reactiveMongoTemplate;

    private FeedService feedService;

    @BeforeEach
    void setUp() {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        feedService = new FeedService(
                feedRepository,
                clock,
                reactiveMongoTemplate
        );
    }

    @Test
    void createsFeedUsingServerClock() {
        when(feedRepository.save(any(Feed.class)))
                .thenAnswer(invocation -> {
                    Feed feed = invocation.getArgument(0);
                    return Mono.just(feed);
                });

        StepVerifier.create(
                        feedService.create(
                                "https://example.com/rss.xml",
                                "Example"
                        )
                )
                .assertNext(feed -> {
                    assertThat(feed.getId()).isNull();
                    assertThat(feed.getUrl())
                            .isEqualTo("https://example.com/rss.xml");
                    assertThat(feed.getTitle()).isEqualTo("Example");
                    assertThat(feed.getCreatedAt()).isEqualTo(STORED_TIME);
                })
                .verifyComplete();
    }

    @Test
    void returnsExistingFeed() {
        Feed feed = new Feed(
                ID,
                "https://example.com/rss.xml",
                "Example",
                NOW
        );

        when(feedRepository.findById(ID))
                .thenReturn(Mono.just(feed));

        StepVerifier.create(feedService.findById(ID))
                .expectNext(feed)
                .verifyComplete();
    }

    @Test
    void failsWhenFeedDoesNotExist() {
        when(feedRepository.findById(ID))
                .thenReturn(Mono.empty());

        StepVerifier.create(feedService.findById(ID))
                .expectErrorSatisfies(error -> {
                    assertThat(error)
                            .isInstanceOf(FeedNotFoundException.class);
                    assertThat(error.getMessage()).contains(ID);
                })
                .verify();
    }

    @Test
    void rejectsMalformedObjectIdWithoutCallingRepository() {
        StepVerifier.create(feedService.findById("abc"))
                .expectErrorSatisfies(error -> {
                    assertThat(error)
                            .isInstanceOf(InvalidFeedIdException.class);
                    assertThat(error.getMessage()).contains("id");
                })
                .verify();

        verifyNoInteractions(feedRepository);
    }
}
