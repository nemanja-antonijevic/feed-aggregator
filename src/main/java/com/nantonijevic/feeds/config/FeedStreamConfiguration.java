package com.nantonijevic.feeds.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(FeedStreamProperties.class)
public class FeedStreamConfiguration {
}
