package com.nantonijevic.feeds.exception;

public class DuplicateFeedUrlException extends RuntimeException {

    public DuplicateFeedUrlException(Throwable cause) {
        super("url already exists", cause);
    }
}
