package com.nantonijevic.feeds.service;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.nantonijevic.feeds.domain.Feed;
import com.nantonijevic.feeds.exception.FeedNotFoundException;
import com.nantonijevic.feeds.exception.InvalidFeedIdException;
import com.nantonijevic.feeds.repository.FeedRepository;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class FeedService {

    private final FeedRepository feedRepository;
    private final Clock clock;

    public FeedService(FeedRepository feedRepository, Clock clock) {
        this.feedRepository = feedRepository;
        this.clock = clock;
    }

    public Mono<Feed> create(String url, String title) {
        Feed feed = new Feed(
                null,
                url,
                title,
                Instant.now(clock).truncatedTo(ChronoUnit.MILLIS)
        );

        return feedRepository.save(feed);
    }

    public Mono<Feed> findById(String id) {
        if (!ObjectId.isValid(id)) {
            return Mono.error(new InvalidFeedIdException(id));
        }

        return feedRepository.findById(id)
                .switchIfEmpty(Mono.defer(() ->
                        Mono.error(new FeedNotFoundException(id))
                ));
    }
}
