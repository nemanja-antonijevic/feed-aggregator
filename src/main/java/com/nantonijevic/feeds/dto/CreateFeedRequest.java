package com.nantonijevic.feeds.dto;

import com.nantonijevic.feeds.validation.AbsoluteHttpUrl;
import jakarta.validation.constraints.NotBlank;

public record CreateFeedRequest(
        @NotBlank(message = "must not be blank")
        @AbsoluteHttpUrl
        String url,

        @NotBlank(message = "must not be blank")
        String title
) {
}
