package com.nantonijevic.feeds.exception;

public class FeedSourceTimeoutException
        extends RuntimeException {

    public FeedSourceTimeoutException(Throwable cause) {
        super("feed source timed out", cause);
    }
}
