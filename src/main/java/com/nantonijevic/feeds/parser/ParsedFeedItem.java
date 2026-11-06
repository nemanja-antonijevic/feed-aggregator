package com.nantonijevic.feeds.parser;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record ParsedFeedItem(
        String guid,
        String title,
        String link,
        Instant publishedAt,
        Map<String, Object> extras
) {

    public ParsedFeedItem {
        extras = extras == null || extras.isEmpty()
                ? Map.of()
                : Collections.unmodifiableMap(
                        new LinkedHashMap<>(extras)
                );
    }

    public ParsedFeedItem(
            String guid,
            String title,
            String link,
            Instant publishedAt
    ) {
        this(guid, title, link, publishedAt, Map.of());
    }
}
