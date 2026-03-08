# Technical Documentation — Kafka Streaming Pipeline

This document records the engineering decisions made during the design and implementation of the real-time order metrics pipeline. It explains why each decision was made, what alternatives were considered, and what would change at production scale.

---

## Table of Contents

1. [Why This Pipeline Exists](#why-this-pipeline-exists)
2. [Kafka Architecture Decisions](#kafka-architecture-decisions)
3. [Producer Design](#producer-design)
4. [Consumer Design](#consumer-design)
5. [Windowing Design](#windowing-design)
6. [Load Design](#load-design)
7. [Offset Management and Delivery Guarantees](#offset-management-and-delivery-guarantees)
8. [Scale Considerations](#scale-considerations)
9. [Known Limitations](#known-limitations)

---

## Why This Pipeline Exists

The first three projects in this series were batch pipelines — each ran on a schedule, processed a bounded dataset, and exited. This model works well when data arrives in discrete chunks (daily CSV exports, daily API pulls, daily log files) and when processing latency of hours is acceptable.

Order processing breaks both assumptions. Orders arrive continuously — there is no natural end to the stream. And revenue anomalies, fraud patterns, and traffic spikes need to be detected in seconds, not hours. A daily batch pipeline that detects a payment processing outage the following morning is operationally useless.

This pipeline introduces stream processing: treat each order event as a discrete unit of work to be processed the moment it arrives, rather than accumulating events and processing them in bulk on a schedule.

The architectural shift this requires is not just technical — it is conceptual. Batch processing asks "what data do I have?" Stream processing asks "what just happened?"

---

## Kafka Architecture Decisions

### Why Kafka Over a Simple Database Queue

A naive approach to streaming would be: write orders to a PostgreSQL queue table, have the consumer poll that table for new rows. This works at low volume but breaks down quickly:

- The database becomes a bottleneck — high-volume writes and polling reads compete for the same connection pool
- Multiple consumers reading the same queue require complex locking to prevent double-processing
- No replay capability — once a row is consumed and deleted, it is gone
- No partition-based ordering guarantees

Kafka solves all of these. It is purpose-built for high-throughput event streaming. Writes are sequential appends to a log — the fastest possible disk operation. Multiple independent consumers read the same log without coordination. Events are retained for 7 days and replayable. Partitions guarantee ordered delivery within a category.

### Why Three Partitions

The topic `orders` is created with three partitions. The number of partitions is the maximum number of consumer instances that can process this topic in parallel — Kafka assigns at most one partition per consumer instance within a group.

Three partitions means the consumer can scale horizontally to three instances at peak load. With five product categories distributed across three partitions via hash routing, the load is distributed reasonably evenly without over-partitioning for a development setup.

In production, partition count is chosen based on target throughput and expected consumer parallelism. It cannot be reduced after creation without data loss — choosing too few partitions early is a common and costly mistake.

### Why Product Category as Partition Key

Every message sent to Kafka with the same key is routed to the same partition by consistent hashing. Using `product_category` as the key means all Electronics orders go to the same partition, all Clothing orders go to the same partition, and so on.

This guarantees **ordering within a category** — if order A was placed before order B, both in the Electronics category, the consumer will always see A before B. This matters for any consumer that needs to detect state transitions within a category (e.g., "revenue dropped from window N to window N+1 in Electronics").

The alternative — a random or null key — distributes messages round-robin across partitions for maximum throughput, but loses ordering guarantees. For an analytics consumer computing per-category aggregations, ordering within a category is more valuable than perfectly balanced partition load.

---

## Producer Design

### Why Faker Over Static Test Data

The producer generates realistic synthetic order events using the Faker library rather than hardcoded test fixtures. Three reasons:

**Volume testing** — the producer runs continuously, generating hundreds of events per minute. Static fixtures would require a large pre-built dataset. Faker generates unbounded data on demand.

**Realistic distribution** — Faker generates plausible product names, realistic price ranges, and diverse geographic data. The resulting metrics look like real operational data, which makes it easier to spot anomalies during development.

**Self-contained** — the pipeline requires no external order service or data file to run. Clone, configure, start — the producer generates its own data.

### Why `acks="all"`

The producer is configured with `acks="all"` — it waits for acknowledgement from all in-sync replicas before considering a send successful. In development with one broker, this makes no difference. In production with multiple brokers, it prevents data loss during broker failover.

The scenario `acks="all"` protects against: the leader broker receives a message, acknowledges it to the producer, and immediately crashes before replicating to followers. With `acks=1` (acknowledge on leader receipt), that message is lost. With `acks="all"`, the producer only receives acknowledgement after all in-sync replicas have the message — a follower can take over with no data loss.

Building with `acks="all"` in development means production code is safe by default rather than requiring a configuration change under pressure.

### Why Callbacks Over Blocking Sends

`producer.send()` is asynchronous — it returns a Future immediately and sends in a background thread. Adding `.add_callback()` and `.add_errback()` handles success and failure without blocking the producer loop.

The alternative — calling `.get()` on the Future — blocks until acknowledgement arrives. At 2 events per second with a 5ms round-trip to Kafka, blocking adds negligible latency. At 10,000 events per second, blocking every send collapses throughput completely.

Callbacks are the correct pattern regardless of current volume — they scale to production throughput without code changes.

---

## Consumer Design

### Why `enable_auto_commit=False`

This is the most critical consumer configuration in the entire pipeline. Understanding why requires understanding what auto-commit does.

With `enable_auto_commit=True` (the default), Kafka commits the consumer's offset on a background timer — every 5 seconds by default. This means: every 5 seconds, Kafka is told "this consumer has processed everything up to offset X," regardless of whether the application has actually finished processing those messages.

The failure scenario this creates:

```
Time 0:   Consumer reads messages at offsets 100-150
Time 3s:  Consumer is aggregating, not yet written to PostgreSQL
Time 5s:  Auto-commit fires — Kafka told "offset 150 processed"
Time 6s:  Consumer crashes during PostgreSQL write
Time 7s:  Consumer restarts — reads from offset 151
Time 7s:  Offsets 100-150 are permanently lost — never written to PostgreSQL
```

With `enable_auto_commit=False`, the consumer commits manually after a successful flush:

```
Time 0:   Consumer reads messages at offsets 100-150
Time 30s: Window expires — aggregate and write to PostgreSQL — SUCCESS
Time 30s: consumer.commit() — Kafka told "offset 150 processed"
Time 31s: If crash before commit: restart reads from offset 100 again
Time 31s: PostgreSQL upsert handles duplicate window — correct final state
```

Manual commit gives complete control over exactly when Kafka considers messages processed. This is non-negotiable for any pipeline where data loss is unacceptable.

### Why the Flush Check Runs After Every Poll

The window expiry check runs after every `poll()` call, not just when messages arrive:

```python
message_batch = consumer.poll(timeout_ms=1000)
# process messages...
if elapsed >= WINDOW_SECONDS:
    flush()
```

During high traffic, `poll()` returns immediately with a full batch. The expiry check runs frequently. During quiet periods, `poll()` blocks for up to `timeout_ms` milliseconds waiting for messages, then returns an empty batch. The expiry check still runs after the timeout.

This ensures windows flush on schedule even during periods of no traffic. Without this, a quiet period would leave a window open indefinitely — metrics would not appear in PostgreSQL until the next burst of events triggered a poll return.

### Why the Consumer Receives the Engine as a Parameter

`run_consumer(engine)` accepts a SQLAlchemy engine from `main.py` rather than creating its own:

```python
# main.py creates one engine
engine = get_engine()
run_consumer(engine)
```

SQLAlchemy engines maintain a connection pool — a set of reusable database connections. Creating multiple engines creates multiple connection pools, wasting database connections. Passing one engine from `main.py` ensures the entire application shares one pool regardless of how many modules need database access.

This is dependency injection — components receive their dependencies rather than creating them internally. It also makes testing easier: tests can inject a test engine without modifying the consumer code.

---

## Windowing Design

### Why Tumbling Windows Over Sliding Windows

A **tumbling window** is a fixed-duration, non-overlapping time interval. Window 1 covers seconds 0-30. Window 2 covers seconds 30-60. No event appears in more than one window.

A **sliding window** advances continuously. A 30-second sliding window evaluated every 10 seconds produces three overlapping windows covering the same events from different perspectives.

Tumbling windows were chosen for three reasons:

**Clean metrics rows** — each `window_start × product_category` combination appears exactly once in the metrics table. A sliding window would produce multiple rows for the same event, requiring downstream queries to understand which window to read.

**Simple idempotency** — an upsert on `window_start × product_category` works cleanly with tumbling windows. With sliding windows, the same event appears in multiple windows — the primary key would need to encode window boundaries differently.

**Appropriate for operational monitoring** — the business question is "how many orders arrived in the last 30 seconds?" not "how many orders arrived in any rolling 30-second period?" Tumbling windows answer the first question directly.

Sliding windows are appropriate when the question involves smoothing — "what is the moving average revenue over the last 5 minutes?" — which is a different analytical requirement.

### Why 30 Seconds

Thirty seconds balances three competing concerns:

**Freshness** — shorter windows mean more recent data in PostgreSQL. A 5-second window would give near-real-time visibility.

**Statistical validity** — at 2 events per second across 5 categories, a 30-second window accumulates ~12 events per category. Averages and aggregations computed on 12 samples are meaningful. A 5-second window produces ~2 events per category — metrics with 2 samples are noise.

**Database write frequency** — each flush writes 5 rows to PostgreSQL. At 30-second intervals that is 10 writes per minute. At 5-second intervals it is 60 writes per minute, increasing database load for marginal freshness improvement.

Thirty seconds is a reasonable default for operational dashboards. Real-time fraud detection systems would use 1-5 second windows with dedicated infrastructure.

---

## Load Design

### Why Upsert for Window Metrics

The `order_metrics` table uses `ON CONFLICT DO UPDATE` — the same upsert pattern as Projects 1 and 3. The reason is identical: idempotency on pipeline reruns.

In streaming, the rerun scenario is not "I ran the pipeline twice by mistake" — it is "the consumer crashed after writing to PostgreSQL but before committing the offset." On restart, the consumer reprocesses the same window's events and attempts to write the same `window_start × product_category` rows again.

Without upsert, this produces duplicate rows or a primary key violation that crashes the load stage. With upsert, the second write simply updates the existing row with identical values — a no-op in terms of data state, a success in terms of pipeline health.

This is at-least-once delivery made safe: events may be processed more than once, but the result is always correct.

---

## Offset Management and Delivery Guarantees

Kafka provides three delivery guarantee levels:

**At-most-once** — commit offset before processing. Messages may be lost if processing fails after commit. Never acceptable for financial or operational data.

**At-least-once** — commit offset after processing. Messages may be reprocessed if the consumer crashes after processing but before committing. Requires idempotent load operations to handle duplicates correctly. This pipeline uses at-least-once.

**Exactly-once** — Kafka transactions guarantee each message is processed exactly once end-to-end. Requires Kafka transactions API and a transactional producer/consumer. Significantly more complex and slower. Used in financial systems where both loss and duplication are unacceptable.

At-least-once with idempotent upserts is the pragmatic choice for analytics pipelines. The complexity cost of exactly-once semantics is not justified when a well-designed upsert produces the same correct result whether a window is processed once or twice.

---

## Scale Considerations

### Consumer Becomes the Bottleneck First

The current consumer processes messages sequentially in a single thread. At 2 events per second (120 per minute, ~240 per 30-second window), aggregation and database writes complete in under 1 second — well within the 30-second window.

At 10x volume (20 events per second, 600 per window), the single consumer instance still handles the load comfortably. The aggregation DataFrame has 600 rows instead of 60 — pandas groupby on 600 rows is negligible.

The real bottleneck emerges when a single consumer instance cannot process messages as fast as they arrive — **consumer lag** begins to grow. Consumer lag is the difference between the latest offset written to a partition and the consumer's current position. Rising lag means the consumer is falling behind.

The solution is horizontal scaling — add consumer instances to the `order_metrics_consumer` group. Kafka automatically rebalances: with 3 partitions and 3 consumer instances, each instance owns one partition. Throughput triples with no code changes.

### Partition Count is a Hard Limit on Parallelism

With 3 partitions, maximum consumer parallelism is 3 instances. A 4th consumer instance would sit idle — Kafka cannot assign it a partition. If 10x volume requires 10 consumer instances, the topic needs 10 partitions.

Partition count cannot be reduced after creation. Choosing partition count at topic creation time requires forecasting maximum expected consumer parallelism — a classic capacity planning problem. The industry convention is to over-provision partitions (create 12 or 24) rather than under-provision, accepting the small overhead of extra partitions.

### PostgreSQL Becomes the Bottleneck at Very High Volume

At very high window flush rates — sub-second windows at high event volume — PostgreSQL upsert throughput becomes the constraint. Each flush is a synchronous database write that blocks the consumer until it completes.

The production solution is async writes — the consumer flushes to a write buffer and a separate thread handles database writes, allowing the consumer to continue processing the next window while the previous one is being written. This introduces complexity around buffer management and failure handling that is not justified at current scale.

---

## Known Limitations

**No dead letter handling for malformed events.** The consumer logs and skips messages that fail JSON deserialization. In production, these should be routed to a `orders.dlq` Kafka topic for investigation and reprocessing, following the dead letter pattern established in Projects 1 and 3.

**No schema enforcement at the broker level.** The producer can emit any JSON structure — the consumer discovers schema mismatches only at processing time. Confluent Schema Registry enforces event schema at the Kafka level, rejecting malformed events before they reach any consumer.

**Single broker, no replication.** The development setup runs one Kafka broker with `replication-factor=1`. In production, a minimum of 3 brokers with `replication-factor=3` is standard. A single broker is a single point of failure — if it goes down, the entire pipeline stops.

**In-memory window state lost on crash.** If the consumer crashes mid-window, the accumulated events in `window_events` are lost from memory. On restart, the consumer reprocesses from the last committed offset — events are not lost from Kafka, but the partial window is reprocessed as part of a new, larger window. This slightly distorts the first window after a restart. Exactly-once semantics with a state store (Apache Flink, Kafka Streams) would eliminate this distortion.

**No consumer lag monitoring.** There is no alerting on rising consumer lag. In production, consumer lag metrics would be exported to Prometheus and alert when lag exceeds a threshold — the first warning that the consumer needs to be scaled.