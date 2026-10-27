package com.nantonijevic.feeds.exception;

public class FeedSourceConnectionException
        extends FeedSourceRequestException {

    public FeedSourceConnectionException(Throwable cause) {
        super("feed source request failed", cause);
    }
}
