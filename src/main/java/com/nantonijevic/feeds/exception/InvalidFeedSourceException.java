package com.nantonijevic.feeds.exception;

public class InvalidFeedSourceException
        extends RuntimeException {

    public InvalidFeedSourceException(String message) {
        super(message);
    }

    public InvalidFeedSourceException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
