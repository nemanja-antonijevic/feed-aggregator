package com.nantonijevic.feeds.parser;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import com.nantonijevic.feeds.exception.InvalidFeedSourceException;
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
                toInstant(entry.getPublishedDate())
        );
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
