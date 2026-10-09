package com.nantonijevic.feeds.domain;

import java.time.Instant;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

@Document(collection = "items")
public class Item {

    @Id
    private String id;

    @Field(targetType = FieldType.OBJECT_ID)
    private final String feedId;

    private final String guid;
    private final String title;
    private final String link;
    private final Instant publishedAt;
    private final Instant fetchedAt;

    public Item(
            String id,
            String feedId,
            String guid,
            String title,
            String link,
            Instant publishedAt,
            Instant fetchedAt
    ) {
        this.id = id;
        this.feedId = feedId;
        this.guid = guid;
        this.title = title;
        this.link = link;
        this.publishedAt = publishedAt;
        this.fetchedAt = fetchedAt;
    }

    public String getId() {
        return id;
    }

    public String getFeedId() {
        return feedId;
    }

    public String getGuid() {
        return guid;
    }

    public String getTitle() {
        return title;
    }

    public String getLink() {
        return link;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public Instant getFetchedAt() {
        return fetchedAt;
    }
}
