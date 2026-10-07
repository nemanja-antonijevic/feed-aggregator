package com.nantonijevic.feeds.validation;

import java.net.URI;
import java.net.URISyntaxException;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class AbsoluteHttpUrlValidator
        implements ConstraintValidator<AbsoluteHttpUrl, String> {

    @Override
    public boolean isValid(
            String value,
            ConstraintValidatorContext context
    ) {
        if (value == null || value.isBlank()) {
            return true;
        }

        try {
            URI uri = new URI(value);
            String scheme = uri.getScheme();

            return uri.isAbsolute()
                    && uri.getHost() != null
                    && (
                            "http".equalsIgnoreCase(scheme)
                                    || "https".equalsIgnoreCase(scheme)
                    );
        } catch (URISyntaxException exception) {
            return false;
        }
    }
}
