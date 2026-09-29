# Batched Kafka publishing

The publisher atomically claims at most `notifyhub.publisher.batch-size` ACCEPTED rows,
ordered by creation time and ID. It submits one Kafka record per request, keyed
by user ID, before waiting for acknowledgments. Kafka can coalesce these records
into its own per-partition network batches.

A short REQUIRES_NEW database transaction selects eligible rows using
FOR UPDATE SKIP LOCKED and assigns a fresh batch token and lease expiry in one
CTE statement. Claimed rows remain ACCEPTED but are excluded from other claims
until their lease expires. The transaction commits before any Kafka call.
Successful IDs are persisted with one conditional bulk update to PUBLISHED,
requiring the current token and an unexpired lease.
PUBLISHED means broker acknowledgment, not device delivery or completed fan-out.
Failed sends remain ACCEPTED. Synchronous send exceptions do not stop other
requests in the batch from being submitted.

## Settings

Configured in application.yml:

| Setting | Default | Meaning |
| --- | --- | --- |
| notifyhub.publisher.batch-size | 500 | Maximum requests retained by one publisher |
| notifyhub.publisher.acknowledgment-wait-ms | 1000 | Shared acknowledgment wait per sweep |
| notifyhub.publisher.poll-delay-ms | 100 | Delay after a sweep finishes |
| notifyhub.publisher.lease-ms | 300000 | Claim lifetime, renewed while the owner is active |

An acknowledgment wait timeout does not cancel the Kafka future. Unresolved
sends are retained across sweeps, and the publisher does not fetch a new batch
until the previous batch is resolved and its successful IDs are persisted.
Each subsequent sweep renews retained claims before processing their outcomes.
Long dispatch loops also renew at one third of the lease duration. Lost claims
are removed locally; stale acknowledgments cannot update another owner's rows.
Synchronous and asynchronous send failures release their owned claims for retry.
This bounds outstanding work even during broker delays. Kafka's delivery timeout
ultimately resolves failed deliveries. If the database update fails, acknowledged
IDs remain in memory for a persistence retry without another send.

The acknowledgment wait does not bound the entire sweep: Kafka send() can block
on metadata or buffer availability according to max.block.ms, and database calls
have their own timeouts. Batch size limits record count, not total payload bytes.
Large payloads and producer buffer pressure must be included in load testing.

## Delivery and scaling limits

This is at-least-once publication. A crash after Kafka acknowledgment but before
the database update can result in a duplicate on restart. Consumers must
idempotently process requestId. Redis/API idempotency is a separate concern.

The in-memory bound applies to one publisher instance. Multiple updated instances
claim disjoint eligible batches. A crashed instance's rows become eligible after
lease expiry. Every renewal, publication update, and release checks both token
and expiry using PostgreSQL time. A paused worker cannot revive an expired lease.
This fences database updates, not Kafka: an already-submitted send can still
complete after its claim expires, so duplicates remain possible. Set the lease
comfortably above acknowledgment wait and scheduler delays; command/DB stalls
or process pauses longer than the lease trade off availability against duplicates.

Deploy the V2 Flyway migration before enabling the new publisher. Stop or disable
all old publishers during rollout: the previous implementation ignores claims.
The migration adds nullable columns and a partial index without rewriting existing
notification content. The index creation uses normal Flyway DDL and should be
scheduled appropriately for a large production table.

A batch of permanently failing rows can starve newer requests; retry backoff,
attempt tracking, and dead-letter handling are follow-up work. Per-user keys
route to a common partition, but application retries can reorder logical events.
Scheduling delivery according to scheduledAt is not implemented by this relay.

## Verification

Run:

```sh
./mvnw -Dtest=NotificationClaimsTests,NotificationSweeperTests,NotificationValidationTests test
```

Publisher unit tests use mocked Kafka and claim storage. They cover dispatch
before acknowledgment, partial failures, retained timed-out sends, progression
beyond one batch, retries, and discarding late acknowledgments after ownership loss.
NotificationClaimsTests uses real PostgreSQL and Flyway migrations to verify
disjoint concurrent claims, skipping a deliberately held row lock, renewal,
crash/expiry recovery, stale-owner fencing, release, and bulk publication updates.
These tests do not establish broker throughput. Measure throughput, acknowledgment
latency, backlog age, CPU, database latency, and failure recovery with real services
before claiming capacity or tuning Kafka batch.size, linger.ms, and compression.type.
