package com.nantonijevic.feeds.repository;

import java.time.Instant;

import com.nantonijevic.feeds.TestcontainersConfiguration;
import com.nantonijevic.feeds.domain.Feed;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.mongodb.test.autoconfigure.DataMongoTest;
import org.springframework.context.annotation.Import;
import reactor.test.StepVerifier;

@DataMongoTest
@Import(TestcontainersConfiguration.class)
class FeedRepositoryIntegrationTest {

    @Autowired
    private FeedRepository feedRepository;

    @BeforeEach
    void cleanDatabase() {
        StepVerifier.create(feedRepository.deleteAll())
                .verifyComplete();
    }

    @Test
    void savesAndFindsFeedById() {
        Instant createdAt = Instant.parse("2026-10-07T07:00:00Z");
        Feed feed = new Feed(
                null,
                "https://example.com/rss.xml",
                "Example",
                createdAt
        );

        StepVerifier.create(
                        feedRepository.save(feed)
                                .flatMap(saved ->
                                        feedRepository.findById(saved.getId())
                                )
                )
                .expectNextMatches(found ->
                        ObjectId.isValid(found.getId())
                                && found.getUrl().equals("https://example.com/rss.xml")
                                && found.getTitle().equals("Example")
                                && found.getCreatedAt().equals(createdAt)
                )
                .verifyComplete();
    }
}
