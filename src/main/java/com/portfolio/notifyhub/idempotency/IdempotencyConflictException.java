package com.portfolio.notifyhub.idempotency;

public class IdempotencyConflictException extends RuntimeException {
    public IdempotencyConflictException() {
        super("Idempotency key was already used for different request content.");
    }
}
