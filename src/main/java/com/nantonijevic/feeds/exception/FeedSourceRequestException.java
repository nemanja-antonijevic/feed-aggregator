package com.nantonijevic.feeds.exception;

public class FeedSourceRequestException
        extends RuntimeException {

    public FeedSourceRequestException(String message) {
        super(message);
    }

    public FeedSourceRequestException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
