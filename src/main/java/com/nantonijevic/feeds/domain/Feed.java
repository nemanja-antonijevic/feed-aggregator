package com.nantonijevic.feeds.domain;

import java.time.Instant;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document(collection = "feeds")
public class Feed {

    @Id
    private String id;

    private final String url;
    private final String title;
    private final Instant createdAt;

    @Field(write = Field.Write.NON_NULL)
    private final PollStatus pollStatus;

    public Feed(String id, String url, String title, Instant createdAt) {
        this(id, url, title, createdAt, null);
    }

    @PersistenceCreator
    public Feed(
            String id,
            String url,
            String title,
            Instant createdAt,
            PollStatus pollStatus
    ) {
        this.id = id;
        this.url = url;
        this.title = title;
        this.createdAt = createdAt;
        this.pollStatus = pollStatus;
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

    public PollStatus getPollStatus() {
        return pollStatus;
    }
}
