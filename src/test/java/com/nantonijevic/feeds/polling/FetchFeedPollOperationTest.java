package com.nantonijevic.feeds.polling;

import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.dto.FetchFeedResponse;
import com.nantonijevic.feeds.exception.FeedNotFoundException;
import com.nantonijevic.feeds.exception.FeedSourceHttpException;
import com.nantonijevic.feeds.service.FetchFeedService;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class FetchFeedPollOperationTest {

    private static final String FEED_ID =
            "507f1f77bcf86cd799439011";

    private final RecordingFetchFeedService fetchFeedService =
            new RecordingFetchFeedService();

    private final FetchFeedPollOperation operation =
            new FetchFeedPollOperation(fetchFeedService);

    @Test
    void deletedFeedIsSkippedWithoutFailure() {
        Feed feed = feed();
        fetchFeedService.result = Mono.error(
                new FeedNotFoundException(FEED_ID)
        );

        StepVerifier.create(operation.poll(feed))
                .verifyComplete();

        assertThat(fetchFeedService.requestedId).isEqualTo(FEED_ID);
    }

    @Test
    void successfulFetchResultIsDiscarded() {
        fetchFeedService.result = Mono.just(
                new FetchFeedResponse(3, 2, 1)
        );

        StepVerifier.create(operation.poll(feed()))
                .verifyComplete();

        assertThat(fetchFeedService.requestedId).isEqualTo(FEED_ID);
    }

    @Test
    void everyOtherErrorPassesThroughAsSameInstance() {
        IllegalStateException error =
                new IllegalStateException("poll failed");
        fetchFeedService.result = Mono.error(error);

        StepVerifier.create(operation.poll(feed()))
                .expectErrorSatisfies(actual ->
                        assertThat(actual).isSameAs(error)
                )
                .verify();
    }

    @Test
    void retryableSourceErrorPassesThroughAsSameInstance() {
        FeedSourceHttpException error =
                new FeedSourceHttpException(503);
        fetchFeedService.result = Mono.error(error);

        StepVerifier.create(operation.poll(feed()))
                .expectErrorSatisfies(actual ->
                        assertThat(actual).isSameAs(error)
                )
                .verify();
    }

    private Feed feed() {
        return new Feed(
                FEED_ID,
                "https://example.com/feed.xml",
                "Example",
                Instant.parse("2026-10-29T08:00:00Z")
        );
    }

    private static final class RecordingFetchFeedService
            extends FetchFeedService {

        private String requestedId;
        private Mono<FetchFeedResponse> result;

        private RecordingFetchFeedService() {
            super(null, null, null, null, null);
        }

        @Override
        public Mono<FetchFeedResponse> fetch(String feedId) {
            requestedId = feedId;
            return result;
        }
    }
}
