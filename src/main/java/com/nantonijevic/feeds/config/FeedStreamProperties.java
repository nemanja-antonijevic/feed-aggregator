package com.nantonijevic.feeds.config;

import java.time.Duration;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("feeds.stream")
public class FeedStreamProperties {

    @NotNull
    private Duration heartbeatInterval = Duration.ofSeconds(15);

    private int bufferCapacity = 256;

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public int getBufferCapacity() {
        return bufferCapacity;
    }

    public void setBufferCapacity(int bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
    }

    @AssertTrue(message = "feeds.stream.heartbeat-interval must be positive")
    public boolean isHeartbeatIntervalValid() {
        return heartbeatInterval != null
                && !heartbeatInterval.isZero()
                && !heartbeatInterval.isNegative();
    }

    @AssertTrue(message = "feeds.stream.buffer-capacity must be positive")
    public boolean isBufferCapacityValid() {
        return bufferCapacity > 0;
    }
}
