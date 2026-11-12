package com.nantonijevic.feeds.exception;

public class ChangeStreamHistoryLostException extends RuntimeException {

    public ChangeStreamHistoryLostException(Throwable cause) {
        super(
                "resume token is no longer available; reconnect without "
                        + "Last-Event-ID to start from now",
                cause
        );
    }
}
