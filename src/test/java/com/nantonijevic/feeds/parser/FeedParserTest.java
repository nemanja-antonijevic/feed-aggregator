package com.nantonijevic.feeds.parser;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FeedParserTest {

    private final FeedParser feedParser = new FeedParser();

    @Test
    void mapsDcCreatorAndRssAuthorToCanonicalCreator() {
        String rss = """
                <rss version="2.0"
                     xmlns:dc="http://purl.org/dc/elements/1.1/">
                  <channel>
                    <title>Example</title>
                    <link>https://example.com</link>
                    <description>Example feed</description>
                    <item>
                      <guid>dc-item</guid>
                      <title>DC item</title>
                      <dc:creator> Ada Lovelace </dc:creator>
                    </item>
                    <item>
                      <guid>rss-item</guid>
                      <title>RSS item</title>
                      <author>grace@example.com</author>
                    </item>
                  </channel>
                </rss>
                """;

        List<ParsedFeedItem> items = feedParser.parse(bytes(rss));

        assertThat(items)
                .extracting(item -> item.extras().get("creator"))
                .containsExactly(
                        "Ada Lovelace",
                        "grace@example.com"
                );
    }

    @Test
    void mapsMultipleCategoriesAndEnclosuresAsVariableShape() {
        String rss = """
                <rss version="2.0">
                  <channel>
                    <title>Example</title>
                    <link>https://example.com</link>
                    <description>Example feed</description>
                    <item>
                      <guid>item-1</guid>
                      <title>Item</title>
                      <category>Java</category>
                      <category>Reactive</category>
                      <enclosure
                          url="https://example.com/audio.mp3"
                          type="audio/mpeg"
                          length="12345"/>
                    </item>
                  </channel>
                </rss>
                """;

        ParsedFeedItem item = feedParser.parse(bytes(rss)).getFirst();

        assertThat(item.extras()).containsEntry(
                "categories",
                List.of("Java", "Reactive")
        );
        assertThat(item.extras()).containsEntry(
                "enclosures",
                List.of(Map.of(
                        "url", "https://example.com/audio.mp3",
                        "type", "audio/mpeg",
                        "length", 12345L
                ))
        );
    }

    @Test
    void omitsBlankCreatorAndAbsentCollections() {
        String rss = """
                <rss version="2.0"
                     xmlns:dc="http://purl.org/dc/elements/1.1/">
                  <channel>
                    <title>Example</title>
                    <link>https://example.com</link>
                    <description>Example feed</description>
                    <item>
                      <guid>item-1</guid>
                      <title>Item</title>
                      <dc:creator>   </dc:creator>
                    </item>
                  </channel>
                </rss>
                """;

        ParsedFeedItem item = feedParser.parse(bytes(rss)).getFirst();

        assertThat(item.extras()).isEmpty();
    }

    @Test
    void omitsNonPositiveEnclosureLength() {
        String rss = """
                <rss version="2.0">
                  <channel>
                    <title>Example</title>
                    <link>https://example.com</link>
                    <description>Example feed</description>
                    <item>
                      <guid>item-1</guid>
                      <title>Item</title>
                      <enclosure
                          url="https://example.com/audio.mp3"
                          type="audio/mpeg"
                          length="-5"/>
                    </item>
                  </channel>
                </rss>
                """;

        ParsedFeedItem item = feedParser.parse(bytes(rss)).getFirst();

        assertThat(item.extras()).containsEntry(
                "enclosures",
                List.of(Map.of(
                        "url", "https://example.com/audio.mp3",
                        "type", "audio/mpeg"
                ))
        );
    }

    @Test
    void omitsCompletelyEmptyEnclosure() {
        String rss = """
                <rss version="2.0">
                  <channel>
                    <title>Example</title>
                    <link>https://example.com</link>
                    <description>Example feed</description>
                    <item>
                      <guid>item-1</guid>
                      <title>Item</title>
                      <enclosure/>
                    </item>
                  </channel>
                </rss>
                """;

        ParsedFeedItem item = feedParser.parse(bytes(rss)).getFirst();

        assertThat(item.extras()).doesNotContainKey("enclosures");
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
