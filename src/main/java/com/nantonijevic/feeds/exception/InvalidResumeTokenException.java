package com.nantonijevic.feeds.exception;

public class InvalidResumeTokenException extends RuntimeException {

    public InvalidResumeTokenException(Throwable cause) {
        super("Last-Event-ID must be a valid resume token", cause);
    }
}
