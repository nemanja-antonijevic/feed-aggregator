package com.nantonijevic.feeds.polling;

import java.time.Instant;

import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.domain.PollOutcome;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class MongoFeedPollStatusStore implements FeedPollStatusStore {

    private final ReactiveMongoTemplate mongoTemplate;

    public MongoFeedPollStatusStore(ReactiveMongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public Mono<Void> recordSuccess(
            String feedId,
            Instant lastAttemptAt,
            Instant lastSuccessAt
    ) {
        Update update = new Update()
                .set("pollStatus.lastAttemptAt", lastAttemptAt)
                .set("pollStatus.lastSuccessAt", lastSuccessAt)
                .set("pollStatus.outcome", PollOutcome.SUCCESS)
                .set("pollStatus.consecutiveFailures", 0)
                .unset("pollStatus.errorType");

        return update(feedId, update);
    }

    @Override
    public Mono<Void> recordFailure(
            String feedId,
            Instant lastAttemptAt,
            String errorType
    ) {
        Update update = new Update()
                .set("pollStatus.lastAttemptAt", lastAttemptAt)
                .set("pollStatus.outcome", PollOutcome.FAILURE)
                .set("pollStatus.errorType", errorType)
                .inc("pollStatus.consecutiveFailures", 1);

        return update(feedId, update);
    }

    private Mono<Void> update(String feedId, Update update) {
        Query query = Query.query(Criteria.where("id").is(feedId));

        return mongoTemplate.updateFirst(query, update, Feed.class)
                .then();
    }
}
