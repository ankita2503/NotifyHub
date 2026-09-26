package com.portfolio.notifyhub.Exception;

public record FieldErrorResponse(
        String field,
        String message
) {}
