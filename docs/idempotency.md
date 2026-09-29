# Idempotency contract

Send an Idempotency-Key header to POST /v1/notifications.

- A new key and valid request return 202 with duplicate=false.
- Repeating a key with matching content returns 200 with the original request ID,
  current stored status, and duplicate=true.
- Repeating a key with different content returns 409 with code
  IDEMPOTENCY_CONFLICT. The existing notification is not changed.

A SHA-256 fingerprint covers userId, category, channels, templateId, payload,
and scheduledAt. JSON payload object keys are sorted recursively, whitespace
is ignored, and equivalent numeric values are normalized. Array order and string
contents remain significant. Channels and template IDs are compared exactly;
channel-list reordering is not normalized. Instants use their normalized Java
Instant representation. Payload remains a JSON-encoded string in the API; invalid
JSON is rejected with 400. Omitted payload and JSON null are equivalent.

Fingerprints are stored in the existing request_hash column. Legacy rows with a
null hash are compared by reconstructing a fingerprint from persisted fields.
No data-clearing migration is needed.

Redis is a hint: a cache hit still reads PostgreSQL to verify content and current
status. A stale cached ID cannot authorize a duplicate response for a missing row.
On cache miss or Redis connection/timeout failure, the database unique constraint
arbitrates concurrent requests. A duplicate insert rolls back before recovery
reads the original row in a fresh transaction. Redis writes happen after commit;
connection failures and timeouts do not reject a committed notification.

Database idempotency lasts as long as the row exists, independently of Redis's
24-hour TTL. Keys currently have global scope; authentication and tenant-scoped
keys remain future work.

Regression checks:

```sh
./mvnw -Dtest=NotificationIdempotencyTests,NotificationValidationTests,NotificationSweeperTests test
```

Idempotency integration tests use real PostgreSQL with mocked Redis. They cover
matching retries, concurrent identical and conflicting requests, changed fields,
JSON normalization, legacy rows, Redis outages, and post-commit cache writes.
