package com.nantonijevic.feeds.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
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
    private final Map<String, Object> extras;

    public Item(
            String id,
            String feedId,
            String guid,
            String title,
            String link,
            Instant publishedAt,
            Instant fetchedAt
    ) {
        this(
                id,
                feedId,
                guid,
                title,
                link,
                publishedAt,
                fetchedAt,
                Map.of()
        );
    }

    @PersistenceCreator
    public Item(
            String id,
            String feedId,
            String guid,
            String title,
            String link,
            Instant publishedAt,
            Instant fetchedAt,
            Map<String, Object> extras
    ) {
        this.id = id;
        this.feedId = feedId;
        this.guid = guid;
        this.title = title;
        this.link = link;
        this.publishedAt = publishedAt;
        this.fetchedAt = fetchedAt;
        this.extras = copyExtras(extras);
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

    public Map<String, Object> getExtras() {
        return Collections.unmodifiableMap(extras);
    }

    private Map<String, Object> copyExtras(
            Map<String, Object> source
    ) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }

        Map<String, Object> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> {
            validateExtraKey(key);
            copy.put(key, copyExtraValue(value));
        });
        return copy;
    }

    private Object copyExtraValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> nested = new LinkedHashMap<>();

            map.forEach((key, nestedValue) -> {
                if (!(key instanceof String stringKey)) {
                    throw new IllegalArgumentException(
                            "extra keys must be strings"
                    );
                }

                validateExtraKey(stringKey);
                nested.put(
                        stringKey,
                        copyExtraValue(nestedValue)
                );
            });

            return nested;
        }

        if (value instanceof List<?> list) {
            List<Object> nested = new ArrayList<>(list.size());
            list.forEach(item -> nested.add(copyExtraValue(item)));
            return nested;
        }

        if (value instanceof Date date) {
            return date.toInstant();
        }

        if (value == null
                || value instanceof String
                || value instanceof Number
                || value instanceof Boolean
                || value instanceof Instant) {
            return value;
        }

        throw new IllegalArgumentException(
                "unsupported extra value type: "
                        + value.getClass().getSimpleName()
        );
    }

    private void validateExtraKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException(
                    "extra key must not be blank"
            );
        }

        if (key.contains(".") || key.startsWith("$")) {
            throw new IllegalArgumentException(
                    "extra key contains unsupported characters"
            );
        }
    }
}
