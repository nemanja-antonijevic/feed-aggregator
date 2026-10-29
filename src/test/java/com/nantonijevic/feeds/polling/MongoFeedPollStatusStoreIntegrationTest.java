package com.nantonijevic.feeds.polling;

import com.nantonijevic.feeds.TestcontainersConfiguration;
import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.domain.PollOutcome;
import com.nantonijevic.feeds.repository.FeedRepository;
import com.nantonijevic.feeds.service.FeedService;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.mongodb.test.autoconfigure.DataMongoTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

@DataMongoTest
@Import({
        TestcontainersConfiguration.class,
        MongoFeedPollStatusStore.class
})
class MongoFeedPollStatusStoreIntegrationTest {

    private static final Instant CREATED_AT =
            Instant.parse("2026-10-28T08:00:00Z");
    private static final Instant FIRST_ATTEMPT =
            Instant.parse("2026-10-28T09:00:00Z");
    private static final Instant FIRST_SUCCESS =
            Instant.parse("2026-10-28T09:00:01Z");

    @Autowired
    private FeedRepository feedRepository;

    @Autowired
    private ReactiveMongoTemplate mongoTemplate;

    @Autowired
    private MongoFeedPollStatusStore statusStore;

    @BeforeEach
    void cleanDatabase() {
        StepVerifier.create(feedRepository.deleteAll()).verifyComplete();
    }

    @Test
    void newlyCreatedFeedOmitsPollStatusFromMongoDocument() {
        FeedService feedService = new FeedService(
                feedRepository,
                Clock.fixed(CREATED_AT, ZoneOffset.UTC),
                mongoTemplate
        );

        StepVerifier.create(
                        feedService.create(
                                        "https://example.com/new.xml",
                                        "New"
                                )
                                .flatMap(feed -> rawFeed(feed.getId()))
                )
                .assertNext(document ->
                        assertThat(document.containsKey("pollStatus"))
                                .isFalse()
                )
                .verifyComplete();
    }

    @Test
    void firstFailureCreatesEmbeddedStatusAndCounter() {
        StepVerifier.create(
                        saveFeed("first-failure")
                                .flatMap(feed -> statusStore.recordFailure(
                                                feed.getId(),
                                                FIRST_ATTEMPT,
                                                "FeedSourceHttpException"
                                        )
                                        .then(Mono.zip(
                                                feedRepository.findById(
                                                        feed.getId()
                                                ),
                                                rawFeed(feed.getId())
                                        ))
                                )
                )
                .assertNext(result -> {
                    Feed feed = result.getT1();
                    Document rawStatus = result.getT2()
                            .get("pollStatus", Document.class);

                    assertThat(feed.getPollStatus()).isNotNull();
                    assertThat(feed.getPollStatus().getLastAttemptAt())
                            .isEqualTo(FIRST_ATTEMPT);
                    assertThat(feed.getPollStatus().getLastSuccessAt())
                            .isNull();
                    assertThat(feed.getPollStatus().getOutcome())
                            .isEqualTo(PollOutcome.FAILURE);
                    assertThat(feed.getPollStatus().getErrorType())
                            .isEqualTo("FeedSourceHttpException");
                    assertThat(feed.getPollStatus().getConsecutiveFailures())
                            .isEqualTo(1);
                    assertThat(rawStatus.containsKey("lastSuccessAt"))
                            .isFalse();
                })
                .verifyComplete();
    }

    @Test
    void parallelFailuresIncrementAtomicallyWithoutReplacingFeedFields() {
        StepVerifier.create(
                        saveFeed("parallel")
                                .flatMap(feed -> Mono.when(
                                                statusStore.recordFailure(
                                                        feed.getId(),
                                                        FIRST_ATTEMPT,
                                                        "TimeoutException"
                                                ),
                                                statusStore.recordFailure(
                                                        feed.getId(),
                                                        FIRST_ATTEMPT,
                                                        "TimeoutException"
                                                )
                                        )
                                        .then(feedRepository.findById(
                                                feed.getId()
                                        )))
                )
                .assertNext(feed -> {
                    assertThat(feed.getPollStatus()
                            .getConsecutiveFailures()).isEqualTo(2);
                    assertThat(feed.getUrl()).isEqualTo(
                            "https://example.com/parallel.xml"
                    );
                    assertThat(feed.getTitle()).isEqualTo("parallel");
                    assertThat(feed.getCreatedAt()).isEqualTo(CREATED_AT);
                })
                .verifyComplete();
    }

    @Test
    void failureAfterSuccessPreservesLastSuccess() {
        Instant secondAttempt =
                Instant.parse("2026-10-28T10:00:00Z");

        StepVerifier.create(
                        saveFeed("preserve-success")
                                .flatMap(feed -> statusStore.recordSuccess(
                                                feed.getId(),
                                                FIRST_ATTEMPT,
                                                FIRST_SUCCESS
                                        )
                                        .then(statusStore.recordFailure(
                                                feed.getId(),
                                                secondAttempt,
                                                "TimeoutException"
                                        ))
                                        .then(feedRepository.findById(
                                                feed.getId()
                                        )))
                )
                .assertNext(feed -> {
                    assertThat(feed.getPollStatus().getLastSuccessAt())
                            .isEqualTo(FIRST_SUCCESS);
                    assertThat(feed.getPollStatus().getLastAttemptAt())
                            .isEqualTo(secondAttempt);
                    assertThat(feed.getPollStatus().getOutcome())
                            .isEqualTo(PollOutcome.FAILURE);
                    assertThat(feed.getPollStatus().getErrorType())
                            .isEqualTo("TimeoutException");
                    assertThat(feed.getPollStatus()
                            .getConsecutiveFailures()).isEqualTo(1);
                })
                .verifyComplete();
    }

    @Test
    void successClearsErrorAndResetsFailureCounter() {
        Instant successAttempt =
                Instant.parse("2026-10-28T11:00:00Z");
        Instant successAt =
                Instant.parse("2026-10-28T11:00:01Z");

        StepVerifier.create(
                        saveFeed("reset")
                                .flatMap(feed -> statusStore.recordFailure(
                                                feed.getId(),
                                                FIRST_ATTEMPT,
                                                "TimeoutException"
                                        )
                                        .then(statusStore.recordSuccess(
                                                feed.getId(),
                                                successAttempt,
                                                successAt
                                        ))
                                        .then(feedRepository.findById(
                                                feed.getId()
                                        )))
                )
                .assertNext(feed -> {
                    assertThat(feed.getPollStatus().getLastAttemptAt())
                            .isEqualTo(successAttempt);
                    assertThat(feed.getPollStatus().getLastSuccessAt())
                            .isEqualTo(successAt);
                    assertThat(feed.getPollStatus().getOutcome())
                            .isEqualTo(PollOutcome.SUCCESS);
                    assertThat(feed.getPollStatus().getErrorType()).isNull();
                    assertThat(feed.getPollStatus()
                            .getConsecutiveFailures()).isZero();
                })
                .verifyComplete();
    }

    @Test
    void missingFeedUpdateDoesNotCreateDocument() {
        String missingId = "507f1f77bcf86cd799439099";

        StepVerifier.create(
                        statusStore.recordFailure(
                                        missingId,
                                        FIRST_ATTEMPT,
                                        "TimeoutException"
                                )
                                .then(feedRepository.count())
                )
                .expectNext(0L)
                .verifyComplete();
    }

    private Mono<Feed> saveFeed(String name) {
        return feedRepository.save(new Feed(
                null,
                "https://example.com/" + name + ".xml",
                name,
                CREATED_AT
        ));
    }

    private Mono<Document> rawFeed(String id) {
        Query query = Query.query(
                Criteria.where("_id").is(new ObjectId(id))
        );
        return mongoTemplate.findOne(query, Document.class, "feeds");
    }
}
