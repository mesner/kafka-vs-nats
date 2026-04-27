import time
import threading
from dataclasses import dataclass
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from config import (
    KAFKA_BOOTSTRAP, KAFKA_TOPIC_DEC, KAFKA_TOPIC_WCM,
    KAFKA_PRODUCER_SAFE, KAFKA_PRODUCER_FAST,
    DEC_BURST_COUNT, WCM_BURST_COUNT,
    BATCH_DRAIN_COUNT,
    SATURATION_RATES, SATURATION_STEP_DURATION, SATURATION_LAG_LIMIT,
)
from metrics import MetricsCollector, ScenarioResult, stamp
from mock_message import generate_dec, generate_wcm


@dataclass
class SaturationStep:
    rate_msg_s: int
    producer_msg_s: float
    consumer_msg_s: float
    lag_at_end: int
    hit_limit: bool


def create_topics(names: list[str]) -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    admin.delete_topics(names)
    time.sleep(1.0)
    futures = admin.create_topics([
        NewTopic(n, num_partitions=1, replication_factor=1) for n in names
    ])
    for _, f in futures.items():
        try:
            f.result()
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise


def delete_topics(names: list[str]) -> None:
    AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP}).delete_topics(names)


def _consumer(topic: str, group_id: str) -> Consumer:
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "fetch.min.bytes": 1,
        "fetch.wait.max.ms": 10,
    })
    c.subscribe([topic])
    return c


def _burst(topic: str, generator, count: int, scenario: str,
           producer_cfg: dict) -> ScenarioResult:
    mc = MetricsCollector(scenario, "kafka")
    received = 0

    def produce_thread():
        p = Producer(producer_cfg)
        for _ in range(count):
            p.produce(topic, value=stamp(generator()))
            p.poll(0)
        p.flush()

    consumer = _consumer(topic, f"bench_{scenario}")
    mc.start()
    t = threading.Thread(target=produce_thread, daemon=True)
    t.start()

    while received < count:
        msg = consumer.poll(5.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                raise RuntimeError(msg.error())
            continue
        mc.record_recv(msg.value())
        consumer.commit(message=msg, asynchronous=False)
        received += 1

    t.join()
    mc.stop()
    consumer.close()
    return mc.result()


def run_dec_burst(fast: bool = False) -> ScenarioResult:
    cfg = KAFKA_PRODUCER_FAST if fast else KAFKA_PRODUCER_SAFE
    label = "dec_burst_fast" if fast else "dec_burst_safe"
    return _burst(KAFKA_TOPIC_DEC, generate_dec, DEC_BURST_COUNT, label, cfg)


def run_wcm_burst(fast: bool = False) -> ScenarioResult:
    cfg = KAFKA_PRODUCER_FAST if fast else KAFKA_PRODUCER_SAFE
    label = "wcm_burst_fast" if fast else "wcm_burst_safe"
    return _burst(KAFKA_TOPIC_WCM, generate_wcm, WCM_BURST_COUNT, label, cfg)


def run_batch_drain(fast: bool = False) -> ScenarioResult:
    """Pre-load BATCH_DRAIN_COUNT messages then measure consumer drain time."""
    cfg = KAFKA_PRODUCER_FAST if fast else KAFKA_PRODUCER_SAFE
    label = "batch_drain_fast" if fast else "batch_drain_safe"

    # Pre-load: produce as fast as possible, no consumer
    p = Producer(cfg)
    for _ in range(BATCH_DRAIN_COUNT):
        p.produce(KAFKA_TOPIC_DEC, value=stamp(generate_dec()))
        p.poll(0)
    p.flush()

    # Drain: consumer starts from offset 0, we time the full drain
    mc = MetricsCollector(label, "kafka")
    consumer = _consumer(KAFKA_TOPIC_DEC, f"bench_{label}")
    mc.start()
    received = 0
    while received < BATCH_DRAIN_COUNT:
        msg = consumer.poll(5.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                raise RuntimeError(msg.error())
            continue
        mc.record_recv(msg.value())
        consumer.commit(message=msg, asynchronous=False)
        received += 1
    mc.stop()
    consumer.close()
    return mc.result()


def run_saturation(fast: bool = False) -> list[SaturationStep]:
    """
    Ramp producer rate through SATURATION_RATES. At each step, run for
    SATURATION_STEP_DURATION seconds and report producer vs consumer throughput
    and end-of-step lag.
    """
    cfg = KAFKA_PRODUCER_FAST if fast else KAFKA_PRODUCER_SAFE
    steps: list[SaturationStep] = []

    for target_rate in SATURATION_RATES:
        # Fresh topic per step so leftover messages from prior steps don't skew lag
        create_topics([KAFKA_TOPIC_DEC])
        consumer = _consumer(KAFKA_TOPIC_DEC, f"bench_sat_{target_rate}")
        produced = 0
        consumed = 0
        interval = 1.0 / target_rate

        def produce_thread():
            nonlocal produced
            p = Producer(cfg)
            next_send = time.perf_counter()
            deadline = time.perf_counter() + SATURATION_STEP_DURATION
            while time.perf_counter() < deadline:
                now = time.perf_counter()
                if now >= next_send:
                    p.produce(KAFKA_TOPIC_DEC, value=stamp(generate_dec()))
                    p.poll(0)
                    produced += 1
                    next_send += interval
                else:
                    time.sleep(min(0.005, next_send - now))
            p.flush()

        start = time.perf_counter()
        t = threading.Thread(target=produce_thread, daemon=True)
        t.start()

        while t.is_alive() or consumed < produced:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                continue
            consumer.commit(message=msg, asynchronous=False)
            consumed += 1

        elapsed = time.perf_counter() - start
        lag = produced - consumed
        hit_limit = lag > SATURATION_LAG_LIMIT
        steps.append(SaturationStep(
            rate_msg_s=target_rate,
            producer_msg_s=produced / elapsed,
            consumer_msg_s=consumed / elapsed,
            lag_at_end=lag,
            hit_limit=hit_limit,
        ))
        consumer.close()
        delete_topics([KAFKA_TOPIC_DEC])
        if hit_limit:
            break

    return steps
