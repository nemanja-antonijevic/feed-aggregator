package com.nantonijevic.feeds.domain;

import java.time.Instant;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "feeds")
public class Feed {

    @Id
    private String id;

    private final String url;
    private final String title;
    private final Instant createdAt;

    public Feed(String id, String url, String title, Instant createdAt) {
        this.id = id;
        this.url = url;
        this.title = title;
        this.createdAt = createdAt;
    }

    public String getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }

    public String getTitle() {
        return title;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
