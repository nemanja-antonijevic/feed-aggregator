package com.nantonijevic.feeds;

import org.springframework.boot.SpringApplication;

public class TestFeedAggregatorApplication {

    public static void main(String[] args) {
        SpringApplication.from(FeedAggregatorApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
