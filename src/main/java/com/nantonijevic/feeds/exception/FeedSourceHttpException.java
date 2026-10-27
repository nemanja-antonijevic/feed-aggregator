package com.nantonijevic.feeds.exception;

public class FeedSourceHttpException
        extends FeedSourceRequestException {

    private final int statusCode;

    public FeedSourceHttpException(int statusCode) {
        super("feed source returned HTTP " + statusCode);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public boolean isServerError() {
        return statusCode >= 500 && statusCode < 600;
    }
}
