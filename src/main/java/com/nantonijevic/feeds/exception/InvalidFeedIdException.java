package com.nantonijevic.feeds.exception;

public class InvalidFeedIdException extends RuntimeException {

    public InvalidFeedIdException(String id) {
        super("id must be a valid ObjectId: " + id);
    }
}
