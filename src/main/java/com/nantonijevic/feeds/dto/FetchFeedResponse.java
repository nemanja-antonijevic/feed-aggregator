package com.nantonijevic.feeds.dto;

public record FetchFeedResponse(
        int fetched,
        int inserted,
        int duplicates
) {
}
