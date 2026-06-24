# kafka-vs-nats

Benchmarks Kafka and NATS/JetStream head-to-head across throughput, batch drain time, and saturation, using realistic HL7 v2.6 medical-device message payloads.

## What it does

Four benchmark scenarios run against both systems:

| Scenario | What it measures |
|---|---|
| **DEC burst** | 5,000 × ~4 KB numeric-vitals messages sent as fast as possible (two Kafka producer configs: `safe` and `fast`) |
| **WCM burst** | 500 × ~175 KB waveform messages (12-lead ECG) sent as fast as possible |
| **Batch drain** | 5,000 DEC messages pre-loaded; consumer drain time from a cold start — the most realistic scenario |
| **Saturation ramp** | Producer rate stepped from 50 → 800 msg/s; stops early if consumer lag exceeds the limit |

Results print to the console as a table and are saved to `results/run_YYYYMMDD_HHMMSS.json`.

## Prerequisites

**Homebrew packages** (the runner starts and stops these automatically):

```
brew install kafka nats-server
```

**Python tooling:**

```
brew install uv
```

Requires Python 3.14+. `uv` will install it automatically if needed.

## Running

```
uv run runner.py
```

Kafka and NATS-server are started before the benchmarks and stopped when they finish. No manual service management needed.

## Tuning

Edit `config.py` to change message counts, rates, or producer settings:

```python
DEC_BURST_COUNT           = 5_000
WCM_BURST_COUNT           = 500
BATCH_DRAIN_COUNT         = 5_000
SATURATION_RATES          = [50, 100, 200, 400, 800]   # msg/s per step
SATURATION_STEP_DURATION  = 15                          # seconds per step
```

To find the throughput ceiling, extend `SATURATION_RATES` (e.g. `[800, 2000, 5000, 10000]`).

## Caveats

- **Kafka burst latency is not representative.** The synchronous consumer poll loop trails the producer in burst mode, inflating per-message latencies. Batch drain throughput is a better proxy for real-world performance.
- **Saturation ceiling not yet found.** Both systems handled 800 msg/s with zero lag at the time of testing.
- **Single-partition Kafka.** All tests use one partition (`replication_factor=1`). Multi-partition tests would show higher Kafka throughput at the cost of ordering guarantees.
- **NATS uses file storage.** Streams use `StorageType.FILE` for parity with Kafka's durable log. Switch to `StorageType.MEMORY` in `nats_bench.py` for a speed-of-light upper bound.

## File overview

| File | Purpose |
|---|---|
| `runner.py` | Entry point — orchestrates scenarios, prints table, writes JSON |
| `kafka_bench.py` | Kafka benchmarks via `confluent-kafka` (synchronous) |
| `nats_bench.py` | NATS/JetStream benchmarks via `nats-py` (asyncio) |
| `services.py` | Starts/stops Kafka and NATS-server via subprocess |
| `mock_message.py` | HL7 message generators |
| `config.py` | All tuneable constants |
| `metrics.py` | `MetricsCollector`, `ScenarioResult`, timing helpers |
