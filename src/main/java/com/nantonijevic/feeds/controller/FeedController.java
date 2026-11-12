package com.nantonijevic.feeds.controller;

import java.net.URI;

import com.nantonijevic.feeds.dto.CreateFeedRequest;
import com.nantonijevic.feeds.dto.DailyItemCountResponse;
import com.nantonijevic.feeds.dto.FeedResponse;
import com.nantonijevic.feeds.dto.FetchFeedResponse;
import com.nantonijevic.feeds.dto.ItemResponse;
import com.nantonijevic.feeds.service.FeedService;
import com.nantonijevic.feeds.service.FetchFeedService;
import com.nantonijevic.feeds.service.ItemStreamService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/feeds")
public class FeedController {

    private final FeedService feedService;

    private final FetchFeedService fetchFeedService;

    private final ItemStreamService itemStreamService;

    public FeedController(
            FeedService feedService,
            FetchFeedService fetchFeedService,
            ItemStreamService itemStreamService
    ) {
        this.feedService = feedService;
        this.fetchFeedService = fetchFeedService;
        this.itemStreamService = itemStreamService;
    }

    @PostMapping
    public Mono<ResponseEntity<FeedResponse>> create(
            @Valid @RequestBody CreateFeedRequest request
    ) {
        return feedService.create(request.url(), request.title())
                .map(feed -> {
                    FeedResponse response = FeedResponse.from(feed);
                    URI location = URI.create("/feeds/" + response.id());

                    return ResponseEntity
                            .created(location)
                            .body(response);
                });
    }

    @GetMapping("/{id}")
    public Mono<FeedResponse> findById(@PathVariable String id) {
        return feedService.findById(id)
                .map(feed -> FeedResponse.from(feed));
    }

    @GetMapping
    public Flux<FeedResponse> findAll() {
        return feedService.findAll()
                .map(FeedResponse::from);
    }

    @GetMapping("/{id}/items/daily-counts")
    public Flux<DailyItemCountResponse> dailyCounts(
            @PathVariable String id
    ) {
        return feedService.dailyItemCounts(id);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> deleteById(@PathVariable String id) {
        return feedService.deleteById(id);
    }

    @PostMapping("/{id}/fetch")
    public Mono<FetchFeedResponse> fetch(@PathVariable String id) {
        return fetchFeedService.fetch(id);
    }

    @GetMapping("/{id}/items/stream")
    public Mono<ResponseEntity<Flux<ServerSentEvent<Object>>>> streamItems(
            @PathVariable String id,
            @RequestHeader(name = "Last-Event-ID", required = false)
            String lastEventId
    ) {
        return feedService.findById(id)
                .map(feed -> ResponseEntity.ok()
                        .contentType(MediaType.TEXT_EVENT_STREAM)
                        .body(itemStreamService.stream(
                                feed.getId(),
                                lastEventId
                        )));
    }
}
