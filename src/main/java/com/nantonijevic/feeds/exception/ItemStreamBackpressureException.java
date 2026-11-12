package com.nantonijevic.feeds.exception;

public class ItemStreamBackpressureException extends RuntimeException {

    public ItemStreamBackpressureException(int capacity, Throwable cause) {
        super(
                "live item stream buffer overflowed (capacity="
                        + capacity + ")",
                cause
        );
    }
}
