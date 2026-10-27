package com.nantonijevic.feeds.client;

import com.nantonijevic.feeds.exception.FeedSourceBodyTooLargeException;
import com.nantonijevic.feeds.exception.FeedSourceConnectionException;
import com.nantonijevic.feeds.exception.FeedSourceHttpException;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.PrematureCloseException;
import reactor.test.StepVerifier;

import java.net.URI;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

class FeedSourceClientTest {

    @Test
    void classifiesServerErrorWithItsStatusCode() {
        FeedSourceClient client = clientReturning(
                ClientResponse.create(HttpStatusCode.valueOf(503)).build()
        );

        StepVerifier.create(client.fetch("https://example.com/feed.xml"))
                .expectErrorSatisfies(error -> {
                    assertThat(error)
                            .isInstanceOf(FeedSourceHttpException.class);

                    FeedSourceHttpException httpError =
                            (FeedSourceHttpException) error;

                    assertThat(httpError.getStatusCode()).isEqualTo(503);
                    assertThat(httpError.isServerError()).isTrue();
                    assertThat(httpError.getMessage())
                            .isEqualTo("feed source returned HTTP 503");
                })
                .verify();
    }

    @Test
    void classifiesClientErrorWithItsStatusCode() {
        FeedSourceClient client = clientReturning(
                ClientResponse.create(HttpStatusCode.valueOf(404)).build()
        );

        StepVerifier.create(client.fetch("https://example.com/feed.xml"))
                .expectErrorSatisfies(error -> {
                    assertThat(error)
                            .isInstanceOf(FeedSourceHttpException.class);

                    FeedSourceHttpException httpError =
                            (FeedSourceHttpException) error;

                    assertThat(httpError.getStatusCode()).isEqualTo(404);
                    assertThat(httpError.isServerError()).isFalse();
                })
                .verify();
    }

    @Test
    void classifiesPrematureCloseAsConnectionFailure() {
        FeedSourceClient client = clientFailingWith(
                PrematureCloseException.TEST_EXCEPTION
        );

        StepVerifier.create(client.fetch("https://example.com/feed.xml"))
                .expectErrorSatisfies(error -> {
                    assertThat(error)
                            .isInstanceOf(
                                    FeedSourceConnectionException.class
                            )
                            .hasMessage("feed source request failed")
                            .hasCause(
                                    PrematureCloseException.TEST_EXCEPTION
                            );
                })
                .verify();
    }

    @Test
    void classifiesDnsFailureAsConnectionFailure() {
        UnknownHostException cause =
                new UnknownHostException("missing.example");
        WebClientRequestException requestError =
                new WebClientRequestException(
                        cause,
                        HttpMethod.GET,
                        URI.create("https://missing.example/feed.xml"),
                        HttpHeaders.EMPTY
                );
        FeedSourceClient client = clientFailingWith(requestError);

        StepVerifier.create(client.fetch("https://example.com/feed.xml"))
                .expectErrorSatisfies(error -> {
                    assertThat(error)
                            .isInstanceOf(
                                    FeedSourceConnectionException.class
                            )
                            .hasMessage("feed source request failed")
                            .hasCause(requestError);
                })
                .verify();
    }

    @Test
    void classifiesOversizedBodySeparatelyFromConnectionFailure() {
        DataBufferLimitException cause =
                new DataBufferLimitException("body too large");
        FeedSourceClient client = clientFailingWith(cause);

        StepVerifier.create(client.fetch("https://example.com/feed.xml"))
                .expectErrorSatisfies(error -> {
                    assertThat(error)
                            .isInstanceOf(
                                    FeedSourceBodyTooLargeException.class
                            )
                            .hasMessage("feed source body exceeds 1 MiB")
                            .hasCause(cause);
                })
                .verify();
    }

    private FeedSourceClient clientReturning(ClientResponse response) {
        return client(request -> Mono.just(response));
    }

    private FeedSourceClient clientFailingWith(Throwable error) {
        return client(request -> Mono.error(error));
    }

    private FeedSourceClient client(ExchangeFunction exchangeFunction) {
        return new FeedSourceClient(
                WebClient.builder()
                        .exchangeFunction(exchangeFunction)
                        .build()
        );
    }
}
