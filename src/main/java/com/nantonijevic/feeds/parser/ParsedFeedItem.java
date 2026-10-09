package com.nantonijevic.feeds.parser;

import java.time.Instant;

public record ParsedFeedItem(
        String guid,
        String title,
        String link,
        Instant publishedAt
) {
}
