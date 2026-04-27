KAFKA_BOOTSTRAP = "localhost:9092"
NATS_URL = "nats://localhost:4222"

KAFKA_TOPIC_DEC = "bench_dec"
KAFKA_TOPIC_WCM = "bench_wcm"
NATS_STREAM_DEC = "BENCH_DEC"
NATS_STREAM_WCM = "BENCH_WCM"

# Burst: peak single-producer throughput
DEC_BURST_COUNT = 5_000
WCM_BURST_COUNT = 500

# Batch drain: pre-load this many messages, then time consumer drain
BATCH_DRAIN_COUNT = 5_000   # DEC only; ~20MB payload

# Throughput saturation: ramp producer msg/s until consumer lag exceeds threshold
SATURATION_RATES = [50, 100, 200, 400, 800]   # msg/s steps (DEC messages)
SATURATION_STEP_DURATION = 15                  # seconds per step
SATURATION_LAG_LIMIT = 500                     # stop ramp if consumer lag exceeds this

# Kafka producer configs: "safe" vs "throughput"
KAFKA_PRODUCER_SAFE = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "all",
    "linger.ms": 0,
    "message.max.bytes": 5_242_880,
}
KAFKA_PRODUCER_FAST = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "1",
    "linger.ms": 5,
    "batch.size": 65_536,
    "compression.type": "lz4",
    "message.max.bytes": 5_242_880,
}

RESULTS_DIR = "results"
