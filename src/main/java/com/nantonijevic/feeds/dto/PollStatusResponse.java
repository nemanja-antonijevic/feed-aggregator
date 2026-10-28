package com.nantonijevic.feeds.dto;

import java.time.Instant;

import com.nantonijevic.feeds.domain.PollStatus;

public record PollStatusResponse(
        Instant lastAttemptAt,
        Instant lastSuccessAt,
        String outcome,
        String errorType,
        int consecutiveFailures
) {

    public static PollStatusResponse from(PollStatus status) {
        if (status == null) {
            return null;
        }

        return new PollStatusResponse(
                status.getLastAttemptAt(),
                status.getLastSuccessAt(),
                status.getOutcome().name(),
                status.getErrorType(),
                status.getConsecutiveFailures()
        );
    }
}
