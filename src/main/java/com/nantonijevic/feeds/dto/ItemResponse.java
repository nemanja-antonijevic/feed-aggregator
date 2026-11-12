package com.nantonijevic.feeds.dto;

import java.time.Instant;
import java.util.Map;

import com.nantonijevic.feeds.domain.Item;

public record ItemResponse(
        String id,
        String feedId,
        String guid,
        String title,
        String link,
        Instant publishedAt,
        Instant fetchedAt,
        Map<String, Object> extras
) {

    public static ItemResponse from(Item item) {
        return new ItemResponse(
                item.getId(),
                item.getFeedId(),
                item.getGuid(),
                item.getTitle(),
                item.getLink(),
                item.getPublishedAt(),
                item.getFetchedAt(),
                item.getExtras()
        );
    }
}
