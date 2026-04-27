import asyncio
import time
from dataclasses import dataclass
import nats
from nats.js.api import StreamConfig, RetentionPolicy, StorageType
from config import (
    NATS_URL, NATS_STREAM_DEC, NATS_STREAM_WCM,
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


async def _create_stream(stream: str, subjects: list[str]) -> None:
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    try:
        await js.delete_stream(stream)
    except Exception:
        pass
    await js.add_stream(StreamConfig(
        name=stream,
        subjects=subjects,
        retention=RetentionPolicy.LIMITS,
        storage=StorageType.FILE,
        max_msgs=-1,
        max_bytes=-1,
    ))
    await nc.close()


async def _delete_stream(stream: str) -> None:
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    try:
        await js.delete_stream(stream)
    except Exception:
        pass
    await nc.close()


def create_stream_sync(stream: str, subjects: list[str]) -> None:
    asyncio.run(_create_stream(stream, subjects))


def delete_stream_sync(stream: str) -> None:
    asyncio.run(_delete_stream(stream))


async def _burst(subject: str, stream: str, generator, count: int,
                 scenario: str) -> ScenarioResult:
    nc = await nats.connect(NATS_URL)
    if nc.max_payload < 256 * 1024:
        await nc.close()
        raise RuntimeError(f"NATS max_payload={nc.max_payload} too small for WCM")
    js = nc.jetstream()
    sub = await js.pull_subscribe(subject, durable=f"bench_{scenario}", stream=stream)

    mc = MetricsCollector(scenario, "nats")
    mc.start()

    async def produce():
        for _ in range(count):
            await js.publish(subject, stamp(generator()))

    async def consume():
        received = 0
        while received < count:
            try:
                msgs = await sub.fetch(min(200, count - received), timeout=10)
                for msg in msgs:
                    mc.record_recv(msg.data)
                    await msg.ack()
                    received += 1
            except nats.errors.TimeoutError:
                continue

    await asyncio.gather(produce(), consume())
    mc.stop()
    await nc.close()
    return mc.result()


async def run_dec_burst() -> ScenarioResult:
    return await _burst(
        f"{NATS_STREAM_DEC}.dec", NATS_STREAM_DEC,
        generate_dec, DEC_BURST_COUNT, "dec_burst",
    )


async def run_wcm_burst() -> ScenarioResult:
    return await _burst(
        f"{NATS_STREAM_WCM}.wcm", NATS_STREAM_WCM,
        generate_wcm, WCM_BURST_COUNT, "wcm_burst",
    )


async def run_batch_drain() -> ScenarioResult:
    """Pre-load BATCH_DRAIN_COUNT messages then measure consumer drain time."""
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    subject = f"{NATS_STREAM_DEC}.dec"

    # Pre-load
    for _ in range(BATCH_DRAIN_COUNT):
        await js.publish(subject, stamp(generate_dec()))

    # Drain
    sub = await js.pull_subscribe(subject, durable="bench_batch_drain", stream=NATS_STREAM_DEC)
    mc = MetricsCollector("batch_drain", "nats")
    mc.start()
    received = 0
    while received < BATCH_DRAIN_COUNT:
        try:
            msgs = await sub.fetch(min(200, BATCH_DRAIN_COUNT - received), timeout=10)
            for msg in msgs:
                mc.record_recv(msg.data)
                await msg.ack()
                received += 1
        except nats.errors.TimeoutError:
            continue
    mc.stop()
    await nc.close()
    return mc.result()


async def run_saturation() -> list[SaturationStep]:
    steps: list[SaturationStep] = []

    for target_rate in SATURATION_RATES:
        # Fresh stream per step so leftover messages from prior steps don't skew lag
        await _create_stream(NATS_STREAM_DEC, [f"{NATS_STREAM_DEC}.dec"])
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        subject = f"{NATS_STREAM_DEC}.dec"
        sub = await js.pull_subscribe(
            subject, durable=f"bench_sat_{target_rate}", stream=NATS_STREAM_DEC
        )

        produced = 0
        consumed = 0
        interval = 1.0 / target_rate
        deadline = time.perf_counter() + SATURATION_STEP_DURATION

        async def produce():
            nonlocal produced
            next_send = time.perf_counter()
            while time.perf_counter() < deadline:
                now = time.perf_counter()
                if now >= next_send:
                    await js.publish(subject, stamp(generate_dec()))
                    produced += 1
                    next_send += interval
                else:
                    await asyncio.sleep(min(0.005, next_send - now))

        async def consume():
            nonlocal consumed
            while time.perf_counter() < deadline + 5:
                try:
                    msgs = await sub.fetch(50, timeout=0.5)
                    for msg in msgs:
                        await msg.ack()
                        consumed += 1
                    if time.perf_counter() >= deadline and consumed >= produced:
                        break
                except nats.errors.TimeoutError:
                    if time.perf_counter() >= deadline and consumed >= produced:
                        break

        start = time.perf_counter()
        await asyncio.gather(produce(), consume())
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
        await nc.close()
        await _delete_stream(NATS_STREAM_DEC)
        if hit_limit:
            break

    return steps
