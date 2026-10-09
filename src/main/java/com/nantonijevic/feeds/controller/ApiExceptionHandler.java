package com.nantonijevic.feeds.controller;

import java.util.Comparator;

import com.nantonijevic.feeds.dto.ApiError;
import com.nantonijevic.feeds.exception.DuplicateFeedUrlException;
import com.nantonijevic.feeds.exception.FeedNotFoundException;
import com.nantonijevic.feeds.exception.FeedSourceRequestException;
import com.nantonijevic.feeds.exception.FeedSourceTimeoutException;
import com.nantonijevic.feeds.exception.InvalidFeedIdException;
import com.nantonijevic.feeds.exception.InvalidFeedSourceException;
import com.nantonijevic.feeds.exception.ItemPersistenceException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(FeedNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    ApiError handleNotFound(FeedNotFoundException exception) {
        return new ApiError(exception.getMessage());
    }

    @ExceptionHandler(InvalidFeedIdException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    ApiError handleInvalidId(InvalidFeedIdException exception) {
        return new ApiError(exception.getMessage());
    }

    @ExceptionHandler(DuplicateFeedUrlException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    ApiError handleDuplicateUrl(
            DuplicateFeedUrlException exception
    ) {
        return new ApiError(exception.getMessage());
    }

    @ExceptionHandler(InvalidFeedSourceException.class)
    @ResponseStatus(HttpStatus.UNPROCESSABLE_CONTENT)
    ApiError handleInvalidFeedSource(
            InvalidFeedSourceException exception
    ) {
        return new ApiError(exception.getMessage());
    }

    @ExceptionHandler(FeedSourceRequestException.class)
    @ResponseStatus(HttpStatus.BAD_GATEWAY)
    ApiError handleFeedSourceRequest(
            FeedSourceRequestException exception
    ) {
        return new ApiError(exception.getMessage());
    }

    @ExceptionHandler(WebExchangeBindException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    ApiError handleValidation(WebExchangeBindException exception) {
        return exception.getFieldErrors()
                .stream()
                .sorted(Comparator.comparing(error -> error.getField()))
                .findFirst()
                .map(error -> new ApiError(
                        error.getField() + " " + error.getDefaultMessage()
                ))
                .orElseGet(() -> new ApiError("request is invalid"));
    }

    @ExceptionHandler(FeedSourceTimeoutException.class)
    @ResponseStatus(HttpStatus.GATEWAY_TIMEOUT)
    ApiError handleFeedSourceTimeout(
            FeedSourceTimeoutException exception
    ) {
        return new ApiError(exception.getMessage());
    }

    @ExceptionHandler(ItemPersistenceException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    ApiError handleItemPersistence(
            ItemPersistenceException exception
    ) {
        return new ApiError(exception.getMessage());
    }
}
