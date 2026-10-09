package com.nantonijevic.feeds.exception;

public class ItemPersistenceException
        extends RuntimeException {

    public ItemPersistenceException(Throwable cause) {
        super("item persistence failed", cause);
    }
}
