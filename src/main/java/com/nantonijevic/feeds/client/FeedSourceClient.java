package com.nantonijevic.feeds.client;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import com.nantonijevic.feeds.exception.FeedSourceRequestException;
import com.nantonijevic.feeds.exception.FeedSourceTimeoutException;
import com.nantonijevic.feeds.exception.InvalidFeedSourceException;
import io.netty.handler.timeout.ReadTimeoutException;
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

    private final WebClient webClient;

    public FeedSourceClient(WebClient webClient) {
        this.webClient = webClient;
    }

    public Mono<byte[]> fetch(String url) {
        return webClient.get()
                .uri(url)
                .exchangeToMono(response -> {
                    int status = response.statusCode().value();

                    if (!response.statusCode().is2xxSuccessful()) {
                        return response.releaseBody()
                                .then(Mono.error(
                                        new FeedSourceRequestException(
                                                "feed source returned HTTP "
                                                        + status
                                        )
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
                                new FeedSourceRequestException(
                                        "feed source body exceeds 1 MiB",
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
                        exception ->
                                exception
                                        instanceof WebClientRequestException
                                        || exception
                                        instanceof PrematureCloseException,
                        exception ->
                                new FeedSourceRequestException(
                                        "feed source request failed",
                                        exception
                                )
                );
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
