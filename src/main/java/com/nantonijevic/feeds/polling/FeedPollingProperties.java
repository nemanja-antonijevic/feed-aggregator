package com.nantonijevic.feeds.polling;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties("feeds.polling")
public class FeedPollingProperties {

    @NotNull
    private Duration interval = Duration.ofMinutes(5);

    public Duration getInterval() {
        return interval;
    }

    public void setInterval(Duration interval) {
        this.interval = interval;
    }

    @AssertTrue(message = "feeds.polling.interval must be at least 1s")
    public boolean isIntervalValid() {
        return interval != null
                && interval.compareTo(Duration.ofSeconds(1)) >= 0;
    }
}
