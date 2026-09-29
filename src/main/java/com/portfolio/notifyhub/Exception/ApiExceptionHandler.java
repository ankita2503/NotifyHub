package com.portfolio.notifyhub.Exception;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;
import java.util.Map;
import com.portfolio.notifyhub.idempotency.IdempotencyConflictException;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(IdempotencyConflictException.class)
    public ResponseEntity<Map<String, String>> handleIdempotencyConflict(IdempotencyConflictException ex) {
        return ResponseEntity.status(409).body(Map.of(
                "code", "IDEMPOTENCY_CONFLICT", "message", ex.getMessage()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ValidationErrorResponse> handleValidation(
            MethodArgumentNotValidException ex) {

        List<FieldErrorResponse> errors = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(error -> new FieldErrorResponse(
                        error.getField(),
                        error.getDefaultMessage()
                ))
                .toList();

        return ResponseEntity.badRequest()
                .body(new ValidationErrorResponse(
                        "VALIDATION_ERROR",
                        "Request validation failed",
                        errors
                ));
    }
}
