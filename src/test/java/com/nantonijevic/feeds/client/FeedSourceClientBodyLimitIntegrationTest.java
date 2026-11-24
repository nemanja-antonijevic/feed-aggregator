package com.nantonijevic.feeds.client;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import com.nantonijevic.feeds.config.FeedClientConfig;
import com.nantonijevic.feeds.exception.FeedSourceBodyTooLargeException;
import io.netty.util.ResourceLeakDetector;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class FeedSourceClientBodyLimitIntegrationTest {

    private static final int BODY_LIMIT_BYTES = 1024 * 1024;

    @Test
    void realWebClientOversizedBodyMatchesDocumentedBehavior()
            throws IOException, InterruptedException {
        ResourceLeakDetector.Level previousLeakDetectionLevel =
                ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        LoggerContext loggerContext =
                (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        DiagnosticAppender appender = new DiagnosticAppender();
        appender.setContext(loggerContext);
        appender.start();
        rootLogger.addAppender(appender);

        try {
            AnnotationConfigApplicationContext context =
                    new AnnotationConfigApplicationContext();
            context.getEnvironment().getPropertySources().addFirst(
                    new MapPropertySource(
                            "m2-body-limit-test",
                            Map.of("feeds.source.allow-private-addresses", "true")
                    )
            );
            context.register(FeedClientConfig.class);
            context.refresh();

            try (context;
                    MockWebServer source = new MockWebServer()) {
                source.start();
                source.enqueue(new MockResponse.Builder()
                        .code(200)
                        .addHeader("Content-Type", "application/rss+xml")
                        .body("x".repeat(BODY_LIMIT_BYTES * 8))
                        .build());

                FeedSourceClient client = new FeedSourceClient(
                        context.getBean(WebClient.class)
                );
                StepVerifier.create(client.fetch(
                                source.url("/large.xml").toString()
                        ))
                        .expectErrorSatisfies(error ->
                                assertThat(error)
                                        .isInstanceOf(
                                                FeedSourceBodyTooLargeException.class
                                        )
                                        .hasMessage(
                                                "feed source body exceeds 1 MiB"
                                        )
                        )
                        .verify(Duration.ofSeconds(10));

                assertThat(source.takeRequest(5, TimeUnit.SECONDS)).isNotNull();
            }

            appender.awaitDiagnostic(Duration.ofSeconds(2));
            assertThat(appender.containsLog("LEAK:"))
                    .as("the paranoid leak detector emits no LEAK report")
                    .isFalse();
            assertThat(appender.containsLog("IllegalReferenceCountException"))
                    .as("no post-termination IllegalReferenceCountException")
                    .isFalse();
        } finally {
            rootLogger.detachAppender(appender);
            appender.stop();
            ResourceLeakDetector.setLevel(previousLeakDetectionLevel);
        }
    }

    private static final class DiagnosticAppender
            extends ListAppender<ILoggingEvent> {

        private final CountDownLatch diagnosticLogged = new CountDownLatch(1);

        @Override
        protected void append(ILoggingEvent event) {
            super.append(event);
            if (contains(event, "LEAK:")
                    || contains(event, "IllegalReferenceCountException")) {
                diagnosticLogged.countDown();
            }
        }

        private void awaitDiagnostic(Duration timeout)
                throws InterruptedException {
            diagnosticLogged.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        private boolean containsLog(String text) {
            return list.stream().anyMatch(event -> contains(event, text));
        }

        private boolean contains(ILoggingEvent event, String text) {
            if (event.getFormattedMessage().contains(text)) {
                return true;
            }

            IThrowableProxy throwable = event.getThrowableProxy();
            while (throwable != null) {
                if (throwable.getClassName().contains(text)
                        || throwable.getMessage().contains(text)) {
                    return true;
                }
                throwable = throwable.getCause();
            }
            return false;
        }
    }
}
