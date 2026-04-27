# Kafka vs NATS/JetStream — Benchmark Report

**Date:** 2026-04-27  
**Host:** macOS 25.1.0, Apple Silicon  
**Kafka:** 4.2.0 (KRaft, single broker, single partition)  
**NATS-server:** Homebrew default, JetStream enabled, file storage  
**Python:** 3.14.2 — `confluent-kafka 2.14`, `nats-py 2.14`

---

## Context

The target system will emit full HL7 v2.6 ORU^R01 messages from a
patient monitor. Two feed types are in scope:

| Feed | Description | Approx. size | Rate |
|---|---|---|---|
| DEC | Numeric vitals (HR, IBP, SpO₂, temp) | ~4 KB | 1 msg / 6 s per device |
| WCM | 12-lead ECG waveforms (1440 samples/lead) | ~175 KB | 1 msg / 6 s per device |

The primary question is whether NATS/JetStream can serve as a drop-in replacement for Kafka,
with emphasis on **sustained throughput** and **batch drain speed** over point latency.

---

## Scenario Results

### 1. DEC Burst — 5,000 × ~4 KB messages

| Config | System | Throughput | MB/s | p50 latency | p99 latency |
|---|---|---|---|---|---|
| safe (`acks=all`, `linger=0`) | Kafka | 1,305 msg/s | 5.40 | 3,467 ms† | 3,651 ms† |
| safe | NATS | **4,776 msg/s** | **19.76** | **0.37 ms** | **0.66 ms** |
| fast (`acks=1`, `linger=5ms`, lz4) | Kafka | 1,340 msg/s | 5.54 | 3,376 ms† | 3,560 ms† |
| fast | NATS | **4,918 msg/s** | **20.34** | **0.36 ms** | **0.61 ms** |

NATS delivers **~3.7× higher throughput** on small-message bursts. The `fast` Kafka config
provides only a ~3% improvement over `safe`, meaning the bottleneck is the consumer poll loop,
not producer batching.

> † **Kafka latency caveat:** These figures reflect a benchmark architecture artifact. The
> synchronous producer thread fills the topic while the consumer polls sequentially behind it.
> In a production Kafka deployment with a dedicated consumer process, p99 latency on 4 KB
> messages would be milliseconds, not seconds. See the Batch Drain section for a more meaningful
> throughput comparison.

---

### 2. WCM Burst — 500 × ~175 KB messages

| System | Throughput | MB/s | p50 latency | p99 latency |
|---|---|---|---|---|
| Kafka (safe) | 121 msg/s | 8.72 | 2,055 ms† | 3,140 ms† |
| NATS | 117 msg/s | 8.48 | **8.67 ms** | **12.16 ms** |

At large payload sizes the two systems are **effectively tied on throughput** (~8.7 MB/s each).
The bottleneck shifts from broker internals to local disk I/O, where both saturate at the same
ceiling. NATS still shows dramatically lower latency per message.

---

### 3. Batch Drain — 5,000 DEC messages pre-loaded, consumer timed from cold start

This is the most operationally relevant scenario: a backlog of messages has accumulated and a
single consumer must drain it as fast as possible.

| System | Drain time | Throughput | MB/s |
|---|---|---|---|
| Kafka (`acks=all`) | 3.61 s | 1,385 msg/s | 5.73 |
| Kafka (`acks=1` + lz4) | 3.60 s | 1,387 msg/s | 5.74 |
| **NATS JetStream** | **0.19 s** | **25,940 msg/s** | **107.3** |

NATS drains the same 5,000-message backlog in **0.19 seconds vs Kafka's 3.6 seconds — an 18.7×
difference.** The Kafka fast config provides no meaningful improvement: at ~5.7 MB/s the
consumer is already at its ceiling regardless of producer settings.

The NATS advantage comes from pull-subscribe batch fetching (`fetch(batch=200)`), which retrieves
up to 200 messages per server round-trip. Kafka's `consumer.poll()` model is also batched but
the Python `confluent-kafka` binding adds per-poll overhead that limits single-consumer
throughput to ~1,400 msg/s on small messages.

---

### 4. Throughput Saturation — DEC messages, rates 50–800 msg/s

Both systems were ramped through increasing producer rates (15 s per step). The consumer lag at
the end of each step was measured. Zero lag means the consumer kept up with the producer.

| Rate (msg/s) | Kafka prod | Kafka cons | Kafka lag | NATS prod | NATS cons | NATS lag |
|---|---|---|---|---|---|---|
| 50 | 49.7 | 49.7 | 0 | 48.4 | 48.4 | 0 |
| 100 | 99.4 | 99.4 | 0 | 96.8 | 96.8 | 0 |
| 200 | 198.6 | 198.6 | 0 | 193.6 | 193.6 | 0 |
| 400 | 397.2 | 397.2 | 0 | 387.1 | 387.1 | 0 |
| 800 | 794.5 | 794.5 | 0 | 774.2 | 774.2 | 0 |

**Neither system showed any consumer lag through 800 msg/s.** For context, a deployment of
~100 concurrent devices each emitting one DEC message every 6 seconds would produce ~17 msg/s —
well below the tested range. Both systems have substantial headroom. The throughput ceiling
has not been found; extending `SATURATION_RATES` to 5,000–10,000 msg/s is recommended.

---

## Feature Comparison

| Feature | Kafka | NATS JetStream |
|---|---|---|
| Delivery guarantee | At-least-once (`acks=all`) | At-least-once (PubAck) |
| Exactly-once | Yes (idempotent + transactions) | No |
| Message replay | Yes (offset seek) | Yes (sequence replay) |
| Consumer groups | Yes (native) | Yes (durable consumers) |
| Push / pull consumers | Pull only | Both |
| Retention | Time / size configurable | Time / size / interest |
| Schema registry | Separate service (Confluent SR) | Not built-in |
| Ordering guarantee | Per partition | Per stream / subject |
| Ops complexity | Higher (KRaft, ACLs, tuning) | Lower (single binary, sane defaults) |
| Cloud / K8s | Manageable but operationally heavy | Leaf-node model, lightweight |

---

## Interpretation and Recommendation

For this workload — whole HL7 messages from a small-to-medium device fleet, batch-consumed by a
single Python process — **NATS JetStream is the stronger choice** based on these benchmarks.

**Where NATS wins:**

- **Batch drain speed is 18.7× faster.** If consumers fall behind and need to catch up, NATS
  clears the backlog dramatically faster.
- **Small-message throughput is ~4× higher** under burst conditions, providing more headroom
  for device fleet growth.
- **Sub-millisecond per-message latency** at moderate rates, versus seconds in the Kafka
  benchmark (though the Kafka latency numbers are inflated by benchmark architecture — see
  caveat above).
- **Single binary, zero external dependencies.** Kafka 4.x with KRaft no longer requires
  ZooKeeper, but still involves more operational surface area.

**Where Kafka holds its ground:**

- **WCM throughput is identical** (~8.7 MB/s). At large payloads both systems hit the same
  local disk ceiling.
- **Exactly-once semantics** are available in Kafka and absent in NATS JetStream. Not critical
  for this use case but worth noting if downstream consumers require it.
- **Ecosystem maturity:** Kafka Connect, Schema Registry, Kafka Streams, and extensive
  Java-native tooling are production-proven at large scale. NATS tooling is lighter but less
  battle-tested for very high-volume enterprise workloads.
- **The existing system is already Kafka-based.** Migration cost and risk should factor into
  any decision.

**Open questions before a final decision:**

1. What does the throughput ceiling look like for each system? Extend `SATURATION_RATES` to
   `[800, 2000, 5000, 10000]` to find the break point.
2. Does NATS JetStream's file storage hold up at the projected daily write volume (~150 GB/day
   equivalent)? A multi-hour sustained load test is warranted.
3. Are multiple consumers (e.g., separate pipelines for different downstream databases) needed?
   Both systems support this, but the operational model differs.

---

*Generated from benchmark run `results/run_20260427_090946.json`.*
