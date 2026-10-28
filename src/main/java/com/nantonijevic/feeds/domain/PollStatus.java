package com.nantonijevic.feeds.domain;

import java.time.Instant;

public class PollStatus {

    private final Instant lastAttemptAt;
    private final Instant lastSuccessAt;
    private final PollOutcome outcome;
    private final String errorType;
    private final int consecutiveFailures;

    public PollStatus(
            Instant lastAttemptAt,
            Instant lastSuccessAt,
            PollOutcome outcome,
            String errorType,
            int consecutiveFailures
    ) {
        this.lastAttemptAt = lastAttemptAt;
        this.lastSuccessAt = lastSuccessAt;
        this.outcome = outcome;
        this.errorType = errorType;
        this.consecutiveFailures = consecutiveFailures;
    }

    public Instant getLastAttemptAt() {
        return lastAttemptAt;
    }

    public Instant getLastSuccessAt() {
        return lastSuccessAt;
    }

    public PollOutcome getOutcome() {
        return outcome;
    }

    public String getErrorType() {
        return errorType;
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }
}
