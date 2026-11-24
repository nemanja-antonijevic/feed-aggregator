package com.nantonijevic.feeds.client;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import com.nantonijevic.feeds.exception.FeedSourceBodyTooLargeException;
import com.nantonijevic.feeds.exception.FeedSourceConnectionException;
import com.nantonijevic.feeds.exception.FeedSourceHttpException;
import com.nantonijevic.feeds.exception.FeedSourceRequestException;
import com.nantonijevic.feeds.exception.FeedSourceTimeoutException;
import com.nantonijevic.feeds.exception.InvalidFeedSourceException;
import io.netty.handler.timeout.ReadTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.PrematureCloseException;

@Component
public class FeedSourceClient {

    private static final Duration REQUEST_TIMEOUT =
            Duration.ofSeconds(3);
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FeedSourceClient.class);

    private final WebClient webClient;

    public FeedSourceClient(WebClient webClient) {
        this.webClient = webClient;
    }

    public Mono<byte[]> fetch(String url) {
        String escapedUrl = url.replace("{", "%7B")
                .replace("}", "%7D");
        URI requestUri = URI.create(escapedUrl);

        return webClient.get()
                .uri(requestUri)
                .exchangeToMono(response -> {
                    int status = response.statusCode().value();

                    if (!response.statusCode().is2xxSuccessful()) {
                        return response.releaseBody()
                                .then(Mono.error(
                                        new FeedSourceHttpException(status)
                                ));
                    }

                    boolean isHtml = response.headers()
                            .contentType()
                            .filter(contentType ->
                                    MediaType.TEXT_HTML
                                            .isCompatibleWith(contentType)
                            )
                            .isPresent();

                    if (isHtml) {
                        return response.releaseBody()
                                .then(Mono.error(
                                        new InvalidFeedSourceException(
                                                "feed source did not return "
                                                        + "an RSS or Atom feed"
                                        )
                                ));
                    }

                    return response.bodyToMono(byte[].class);
                })
                .timeout(REQUEST_TIMEOUT)
                .onErrorMap(
                        DataBufferLimitException.class,
                        exception ->
                                new FeedSourceBodyTooLargeException(
                                        exception
                                )
                )
                .onErrorMap(
                        TimeoutException.class,
                        FeedSourceTimeoutException::new
                )
                .onErrorMap(
                        this::hasReadTimeout,
                        FeedSourceTimeoutException::new
                )
                .onErrorMap(
                        exception -> exception
                                instanceof WebClientRequestException
                                && findInvalidSource(exception) != null,
                        this::findInvalidSource
                )
                .onErrorMap(
                        exception ->
                                exception
                                        instanceof WebClientRequestException
                                        || exception
                                        instanceof PrematureCloseException,
                        exception ->
                                new FeedSourceConnectionException(
                                        exception
                                )
                )
                .onErrorMap(
                        exception -> !isFeedSourceException(exception),
                        exception -> {
                            LOGGER.warn(
                                    "Unmapped feed source exception type: {}",
                                    exception.getClass().getName()
                            );
                            return new FeedSourceRequestException(
                                    "feed source request failed",
                                    exception
                            );
                        }
                );
    }

    private boolean isFeedSourceException(Throwable exception) {
        return exception instanceof FeedSourceRequestException
                || exception instanceof FeedSourceTimeoutException
                || exception instanceof InvalidFeedSourceException;
    }

    // The resolver refuses a blocked address; WebClient wraps that refusal.
    private InvalidFeedSourceException findInvalidSource(Throwable throwable) {
        Throwable current = throwable;

        while (current != null) {
            if (current instanceof InvalidFeedSourceException invalidSource) {
                return invalidSource;
            }

            current = current.getCause();
        }

        return null;
    }

    private boolean hasReadTimeout(Throwable throwable) {
        Throwable current = throwable;

        while (current != null) {
            if (current instanceof ReadTimeoutException) {
                return true;
            }

            current = current.getCause();
        }

        return false;
    }
}
