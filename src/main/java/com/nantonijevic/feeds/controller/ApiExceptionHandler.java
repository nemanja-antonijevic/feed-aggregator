package com.nantonijevic.feeds.controller;

import java.util.Comparator;

import com.nantonijevic.feeds.dto.ApiError;
import com.nantonijevic.feeds.exception.FeedNotFoundException;
import com.nantonijevic.feeds.exception.InvalidFeedIdException;
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
}
