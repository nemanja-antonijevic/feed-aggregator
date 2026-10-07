package com.nantonijevic.feeds.dto;

import java.time.Instant;

import com.nantonijevic.feeds.domain.Feed;

public record FeedResponse(
        String id,
        String url,
        String title,
        Instant createdAt
) {

    public static FeedResponse from(Feed feed) {
        return new FeedResponse(
                feed.getId(),
                feed.getUrl(),
                feed.getTitle(),
                feed.getCreatedAt()
        );
    }
}
