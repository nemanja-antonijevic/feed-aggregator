package com.nantonijevic.feeds.parser;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.nantonijevic.feeds.exception.InvalidFeedSourceException;
import com.rometools.rome.feed.synd.SyndCategory;
import com.rometools.rome.feed.synd.SyndEnclosure;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;
import org.springframework.stereotype.Component;

@Component
public class FeedParser {

    public List<ParsedFeedItem> parse(byte[] body) {
        try (
                XmlReader reader = new XmlReader(
                        new ByteArrayInputStream(body)
                )
        ) {
            SyndFeedInput input = new SyndFeedInput();
            input.setAllowDoctypes(false);

            SyndFeed feed = input.build(reader);

            return feed.getEntries()
                    .stream()
                    .map(this::toParsedItem)
                    .toList();
        } catch (InvalidFeedSourceException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new InvalidFeedSourceException(
                    "feed source returned an invalid feed",
                    exception
            );
        }
    }

    private ParsedFeedItem toParsedItem(SyndEntry entry) {
        String link = nullableText(entry.getLink());
        String guid = nullableText(entry.getUri());

        if (guid == null) {
            guid = link;
        }

        if (guid == null) {
            throw new InvalidFeedSourceException(
                    "feed source returned an invalid feed"
            );
        }

        return new ParsedFeedItem(
                guid,
                nullableText(entry.getTitle()),
                link,
                toInstant(entry.getPublishedDate()),
                extras(entry)
        );
    }

    private Map<String, Object> extras(SyndEntry entry) {
        Map<String, Object> extras = new LinkedHashMap<>();

        String creator = nullableText(entry.getAuthor());
        if (creator != null) {
            extras.put("creator", creator);
        }

        List<String> categories = entry.getCategories()
                .stream()
                .map(SyndCategory::getName)
                .map(this::nullableText)
                .filter(value -> value != null)
                .toList();
        if (!categories.isEmpty()) {
            extras.put("categories", categories);
        }

        List<Map<String, Object>> enclosures = entry.getEnclosures()
                .stream()
                .map(this::enclosure)
                .filter(value -> !value.isEmpty())
                .toList();
        if (!enclosures.isEmpty()) {
            extras.put("enclosures", enclosures);
        }

        return extras;
    }

    private Map<String, Object> enclosure(SyndEnclosure enclosure) {
        Map<String, Object> values = new LinkedHashMap<>();

        String url = nullableText(enclosure.getUrl());
        if (url != null) {
            values.put("url", url);
        }

        String type = nullableText(enclosure.getType());
        if (type != null) {
            values.put("type", type);
        }

        if (enclosure.getLength() > 0) {
            values.put("length", enclosure.getLength());
        }

        return values;
    }

    private String nullableText(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        return value.trim();
    }

    private Instant toInstant(Date value) {
        return value == null ? null : value.toInstant();
    }
}
