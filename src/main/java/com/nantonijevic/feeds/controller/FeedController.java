package com.nantonijevic.feeds.controller;

import java.net.URI;

import com.nantonijevic.feeds.dto.CreateFeedRequest;
import com.nantonijevic.feeds.dto.FeedResponse;
import com.nantonijevic.feeds.service.FeedService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/feeds")
public class FeedController {

    private final FeedService feedService;

    public FeedController(FeedService feedService) {
        this.feedService = feedService;
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

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> deleteById(@PathVariable String id) {
        return feedService.deleteById(id);
    }
}
