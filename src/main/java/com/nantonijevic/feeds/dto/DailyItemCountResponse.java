package com.nantonijevic.feeds.dto;

public record DailyItemCountResponse(
        String feedId,
        String day,
        long count
) {
}
