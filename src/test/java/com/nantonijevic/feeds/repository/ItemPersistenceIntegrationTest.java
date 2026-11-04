package com.nantonijevic.feeds.repository;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.MongoWriteException;
import com.nantonijevic.feeds.TestcontainersConfiguration;
import com.nantonijevic.feeds.domain.Item;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class ItemPersistenceIntegrationTest {

    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    private ItemRepository itemRepository;

    @BeforeEach
    void cleanItems() {
        StepVerifier.create(itemRepository.deleteAll())
                .verifyComplete();
    }

    @Test
    void schemaRejectsDocumentWithInvalidGuidType() {
        Document invalidItem = new Document()
                .append("feedId", new ObjectId())
                .append("guid", 42)
                .append("fetchedAt", Date.from(Instant.now()));

        StepVerifier.create(insert(invalidItem))
                .expectErrorSatisfies(this::assertSchemaValidationError)
                .verify();
    }

    @Test
    void schemaRejectsEmptyGuid() {
        Document invalidItem = validItemDocument()
                .append("guid", "");

        StepVerifier.create(insert(invalidItem))
                .expectErrorSatisfies(this::assertSchemaValidationError)
                .verify();
    }

    @Test
    void schemaRejectsDocumentWithoutFeedId() {
        Document invalidItem = validItemDocument();
        invalidItem.remove("feedId");

        StepVerifier.create(insert(invalidItem))
                .expectErrorSatisfies(this::assertSchemaValidationError)
                .verify();
    }

    @Test
    void schemaRejectsDocumentWithoutGuid() {
        Document invalidItem = validItemDocument();
        invalidItem.remove("guid");

        StepVerifier.create(insert(invalidItem))
                .expectErrorSatisfies(this::assertSchemaValidationError)
                .verify();
    }

    @Test
    void schemaRejectsDocumentWithoutFetchedAt() {
        Document invalidItem = validItemDocument();
        invalidItem.remove("fetchedAt");

        StepVerifier.create(insert(invalidItem))
                .expectErrorSatisfies(this::assertSchemaValidationError)
                .verify();
    }

    @Test
    void schemaRejectsFeedIdThatIsNotAnObjectId() {
        Document invalidItem = validItemDocument()
                .append("feedId", new ObjectId().toHexString());

        StepVerifier.create(insert(invalidItem))
                .expectErrorSatisfies(this::assertSchemaValidationError)
                .verify();
    }

    @Test
    void schemaRejectsFetchedAtThatIsNotADate() {
        Document invalidItem = validItemDocument()
                .append("fetchedAt", "2026-11-03T08:00:00Z");

        StepVerifier.create(insert(invalidItem))
                .expectErrorSatisfies(this::assertSchemaValidationError)
                .verify();
    }

    @Test
    void schemaRejectsExtrasThatAreNotAnObject() {
        Document invalidItem = validItemDocument()
                .append("extras", "not-an-object");

        StepVerifier.create(insert(invalidItem))
                .expectErrorSatisfies(this::assertSchemaValidationError)
                .verify();
    }

    @Test
    void readsLegacyItemWithoutExtrasAsEmptyMap() {
        Document legacyItem = validItemDocument();

        StepVerifier.create(
                        reactiveMongoTemplate
                                .getCollection("items")
                                .flatMap(collection -> Mono.from(
                                        collection.insertOne(legacyItem)
                                ))
                                .then(itemRepository.findById(
                                        legacyItem.getObjectId("_id")
                                                .toHexString()
                                ))
                )
                .assertNext(found ->
                        assertThat(found.getExtras()).isEmpty()
                )
                .verifyComplete();
    }

    @Test
    void roundTripsNestedExtras() {
        String feedId = new ObjectId().toHexString();
        Map<String, Object> extras = Map.of(
                "author",
                "Ada Lovelace",
                "categories",
                List.of("java", "reactor"),
                "enclosure",
                Map.of(
                        "url",
                        "https://example.com/audio.mp3",
                        "length",
                        2048
                )
        );
        Item item = item(feedId, extras);

        StepVerifier.create(
                        itemRepository.save(item)
                                .flatMap(saved ->
                                        itemRepository.findById(saved.getId())
                                )
                )
                .assertNext(found ->
                        assertThat(found.getExtras())
                                .isEqualTo(extras)
                )
                .verifyComplete();
    }

    @Test
    void rejectsExtraKeyContainingDot() {
        assertThatThrownBy(() -> item(
                new ObjectId().toHexString(),
                Map.of("dc.creator", "Ada Lovelace")
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("extra key contains unsupported characters");
    }

    @Test
    void rejectsNestedExtraKeyStartingWithDollar() {
        assertThatThrownBy(() -> item(
                new ObjectId().toHexString(),
                Map.of(
                        "enclosure",
                        Map.of("$url", "https://example.com/audio.mp3")
                )
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("extra key contains unsupported characters");
    }

    @Test
    void acceptsSupportedExtraValueTypes() {
        Map<String, Object> extras = new LinkedHashMap<>();
        extras.put("text", "value");
        extras.put("number", 42);
        extras.put("flag", true);
        extras.put("missing", null);
        extras.put("timestamp", Instant.parse("2026-11-03T08:00:00Z"));
        extras.put("nested", Map.of("items", List.of("one", "two")));

        Item item = item(new ObjectId().toHexString(), extras);

        StepVerifier.create(
                        itemRepository.save(item)
                                .flatMap(saved ->
                                        itemRepository.findById(saved.getId())
                                )
                )
                .assertNext(found ->
                        assertThat(found.getExtras())
                                .isEqualTo(extras)
                )
                .verifyComplete();
    }

    @Test
    void rejectsUnsupportedExtraValueType() {
        assertThatThrownBy(() -> item(
                new ObjectId().toHexString(),
                Map.of("unsupported", new UnsupportedExtra("value"))
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "unsupported extra value type: UnsupportedExtra"
                );
    }

    private Item item(
            String feedId,
            Map<String, Object> extras
    ) {
        return new Item(
                null,
                feedId,
                "item-guid",
                "Item title",
                "https://example.com/items/item-guid",
                Instant.parse("2026-11-01T08:00:00Z"),
                Instant.parse("2026-11-03T08:00:00Z"),
                extras
        );
    }

    private Document validItemDocument() {
        return new Document("_id", new ObjectId())
                .append("feedId", new ObjectId())
                .append("guid", "legacy-guid")
                .append("fetchedAt", Date.from(
                        Instant.parse("2026-11-03T08:00:00Z")
                ));
    }

    private Mono<Void> insert(Document document) {
        return reactiveMongoTemplate
                .getCollection("items")
                .flatMap(collection -> Mono.from(
                        collection.insertOne(document)
                ))
                .then();
    }

    private void assertSchemaValidationError(Throwable error) {
        assertThat(error).isInstanceOf(MongoWriteException.class);
        assertThat(((MongoWriteException) error)
                .getError()
                .getCode()).isEqualTo(121);
    }

    private record UnsupportedExtra(String value) {
    }
}
