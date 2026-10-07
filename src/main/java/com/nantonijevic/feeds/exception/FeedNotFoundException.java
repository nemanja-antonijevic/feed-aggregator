package com.nantonijevic.feeds.exception;

public class FeedNotFoundException extends RuntimeException {

    public FeedNotFoundException(String id) {
        super("feed not found: " + id);
    }
}
