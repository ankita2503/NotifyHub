package com.portfolio.notifyhub.Exception;

import java.util.List;

public record ValidationErrorResponse(
        String code,
        String message,
        List<FieldErrorResponse> errors
) {}
