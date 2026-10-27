package com.nantonijevic.feeds.exception;

public class FeedSourceBodyTooLargeException
        extends FeedSourceRequestException {

    public FeedSourceBodyTooLargeException(Throwable cause) {
        super("feed source body exceeds 1 MiB", cause);
    }
}
