import asyncio
import json
import os
import datetime
import services
import kafka_bench
import nats_bench
from config import (
    KAFKA_TOPIC_DEC, KAFKA_TOPIC_WCM,
    NATS_STREAM_DEC, NATS_STREAM_WCM,
    RESULTS_DIR,
)
from metrics import ScenarioResult

FEATURE_TABLE = [
    ("Delivery guarantee",  "At-least-once (acks=all)",         "At-least-once (PubAck)"),
    ("Exactly-once",        "Yes (idempotent + transactions)",   "No (not supported)"),
    ("Message replay",      "Yes (offset seek)",                 "Yes (sequence replay)"),
    ("Consumer groups",     "Yes (native)",                      "Yes (durable consumers)"),
    ("Push / pull",         "Pull only",                         "Both push and pull"),
    ("Retention policy",    "Time / size configurable",          "Time / size / interest"),
    ("Schema registry",     "Separate (Confluent SR)",           "Not built-in"),
    ("Ordering",            "Per partition",                     "Per stream / subject"),
    ("Ops complexity",      "Higher (ZK/KRaft, ACLs)",           "Lower (single binary)"),
    ("Cloud native",        "Kafka on K8s (complex)",            "Leaf nodes, K8s-friendly"),
]


def _setup_kafka_dec():
    kafka_bench.create_topics([KAFKA_TOPIC_DEC])


def _setup_kafka_wcm():
    kafka_bench.create_topics([KAFKA_TOPIC_WCM])


def _teardown_kafka_dec():
    kafka_bench.delete_topics([KAFKA_TOPIC_DEC])


def _teardown_kafka_wcm():
    kafka_bench.delete_topics([KAFKA_TOPIC_WCM])


def _setup_nats_dec():
    nats_bench.create_stream_sync(NATS_STREAM_DEC, [f"{NATS_STREAM_DEC}.dec"])


def _setup_nats_wcm():
    nats_bench.create_stream_sync(NATS_STREAM_WCM, [f"{NATS_STREAM_WCM}.wcm"])


def _teardown_nats_dec():
    nats_bench.delete_stream_sync(NATS_STREAM_DEC)


def _teardown_nats_wcm():
    nats_bench.delete_stream_sync(NATS_STREAM_WCM)


def _run_burst_pair(
    label: str,
    kafka_fn, nats_fn,
    kafka_setup, kafka_teardown,
    nats_setup, nats_teardown,
    fast: bool = False,
) -> tuple[ScenarioResult, ScenarioResult]:
    tag = f"{label}_{'fast' if fast else 'safe'}"
    print(f"  [{tag}] Kafka...", end="", flush=True)
    kafka_setup()
    k = kafka_fn(fast=fast)
    kafka_teardown()
    print(f" {k.throughput_msg_s:.0f} msg/s  {k.throughput_mb_s:.2f} MB/s")

    print(f"  [{tag}] NATS... ", end="", flush=True)
    nats_setup()
    n = asyncio.run(nats_fn())
    nats_teardown()
    print(f" {n.throughput_msg_s:.0f} msg/s  {n.throughput_mb_s:.2f} MB/s")
    return k, n


def print_burst_table(pairs: list[tuple[ScenarioResult, ScenarioResult]]) -> None:
    col = 13
    hdr = (
        f"{'Scenario':<24} {'K msg/s':>{col}} {'N msg/s':>{col}}"
        f" {'K MB/s':>{col}} {'N MB/s':>{col}}"
        f" {'K p50ms':>{col}} {'N p50ms':>{col}}"
        f" {'K p99ms':>{col}} {'N p99ms':>{col}}"
    )
    sep = "=" * len(hdr)
    print(f"\n{sep}\nTHROUGHPUT RESULTS\n{sep}")
    print(hdr)
    print("-" * len(hdr))
    for k, n in pairs:
        print(
            f"{k.scenario:<24}"
            f" {k.throughput_msg_s:>{col}.1f}"
            f" {n.throughput_msg_s:>{col}.1f}"
            f" {k.throughput_mb_s:>{col}.3f}"
            f" {n.throughput_mb_s:>{col}.3f}"
            f" {k.p50_ms:>{col}.2f}"
            f" {n.p50_ms:>{col}.2f}"
            f" {k.p99_ms:>{col}.2f}"
            f" {n.p99_ms:>{col}.2f}"
        )
    print(sep)


def print_batch_drain_table(
    k_safe: ScenarioResult, n: ScenarioResult, k_fast: ScenarioResult
) -> None:
    count = k_safe.msg_count
    print(f"\n{'='*60}")
    print(f"BATCH DRAIN  ({count:,} DEC messages pre-loaded)")
    print(f"{'='*60}")
    rows = [
        ("Kafka (safe acks=all)", k_safe),
        ("Kafka (fast acks=1+lz4)", k_fast),
        ("NATS JetStream", n),
    ]
    for label, r in rows:
        print(
            f"  {label:<26}  {r.elapsed_s:6.2f}s  "
            f"{r.throughput_msg_s:8.1f} msg/s  {r.throughput_mb_s:6.3f} MB/s"
        )
    print(f"{'='*60}")


def print_saturation_table(
    kafka_steps: list, nats_steps: list, fast: bool
) -> None:
    label = "fast" if fast else "safe"
    print(f"\n{'='*72}")
    print(f"THROUGHPUT SATURATION  (DEC messages, Kafka={label})")
    print(f"{'='*72}")
    print(f"  {'Rate':>6}  {'K prod':>8}  {'K cons':>8}  {'K lag':>7}"
          f"  {'N prod':>8}  {'N cons':>8}  {'N lag':>7}")
    print(f"  {'-'*6}  {'-'*8}  {'-'*8}  {'-'*7}  {'-'*8}  {'-'*8}  {'-'*7}")

    all_rates = sorted({s.rate_msg_s for s in kafka_steps} |
                       {s.rate_msg_s for s in nats_steps})
    k_by_rate = {s.rate_msg_s: s for s in kafka_steps}
    n_by_rate = {s.rate_msg_s: s for s in nats_steps}

    for rate in all_rates:
        ks = k_by_rate.get(rate)
        ns = n_by_rate.get(rate)
        kp = f"{ks.producer_msg_s:8.1f}" if ks else f"{'—':>8}"
        kc = f"{ks.consumer_msg_s:8.1f}" if ks else f"{'—':>8}"
        kl = f"{ks.lag_at_end:>7}" if ks else f"{'—':>7}"
        np_ = f"{ns.producer_msg_s:8.1f}" if ns else f"{'—':>8}"
        nc_ = f"{ns.consumer_msg_s:8.1f}" if ns else f"{'—':>8}"
        nl = f"{ns.lag_at_end:>7}" if ns else f"{'—':>7}"
        flag = " !" if (ks and ks.hit_limit) or (ns and ns.hit_limit) else ""
        print(f"  {rate:>6}  {kp}  {kc}  {kl}  {np_}  {nc_}  {nl}{flag}")

    print(f"  ! = hit lag limit ({__import__('config').SATURATION_LAG_LIMIT} msgs)")
    print(f"{'='*72}")


def print_feature_table() -> None:
    w0, w1, w2 = 24, 38, 36
    sep = "=" * (w0 + w1 + w2 + 6)
    print(f"\n{sep}\nFEATURE COMPARISON\n{sep}")
    print(f"{'Feature':<{w0}}  {'Kafka':<{w1}}  {'NATS JetStream':<{w2}}")
    print("-" * (w0 + w1 + w2 + 6))
    for feat, kafka, nats in FEATURE_TABLE:
        print(f"{feat:<{w0}}  {kafka:<{w1}}  {nats:<{w2}}")
    print(sep)


def save_results(data: dict) -> str:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(RESULTS_DIR, f"run_{ts}.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    return path


def main() -> None:
    print("\nKafka vs NATS/JetStream Benchmark")
    print("==================================")
    results: dict = {"burst": [], "batch_drain": {}, "saturation": {}}

    # ── 1. Burst throughput ───────────────────────────────────────────────────
    print("\n─── 1/4  DEC burst (safe) ────────────────────────────────────────")
    dec_safe = _run_burst_pair(
        "dec", kafka_bench.run_dec_burst, nats_bench.run_dec_burst,
        _setup_kafka_dec, _teardown_kafka_dec,
        _setup_nats_dec, _teardown_nats_dec,
        fast=False,
    )

    print("\n─── 2/4  DEC burst (fast) ────────────────────────────────────────")
    dec_fast = _run_burst_pair(
        "dec", kafka_bench.run_dec_burst, nats_bench.run_dec_burst,
        _setup_kafka_dec, _teardown_kafka_dec,
        _setup_nats_dec, _teardown_nats_dec,
        fast=True,
    )

    print("\n─── 3/4  WCM burst (~175KB msgs) ────────────────────────────────")
    wcm_safe = _run_burst_pair(
        "wcm", kafka_bench.run_wcm_burst, nats_bench.run_wcm_burst,
        _setup_kafka_wcm, _teardown_kafka_wcm,
        _setup_nats_wcm, _teardown_nats_wcm,
        fast=False,
    )

    results["burst"] = [
        {"kafka": dec_safe[0].to_dict(), "nats": dec_safe[1].to_dict()},
        {"kafka": dec_fast[0].to_dict(), "nats": dec_fast[1].to_dict()},
        {"kafka": wcm_safe[0].to_dict(), "nats": wcm_safe[1].to_dict()},
    ]

    # ── 2. Batch drain ────────────────────────────────────────────────────────
    print("\n─── 4/4  Batch drain ────────────────────────────────────────────")
    print("  Pre-loading messages for Kafka (safe)...", end="", flush=True)
    _setup_kafka_dec()
    k_drain_safe = kafka_bench.run_batch_drain(fast=False)
    _teardown_kafka_dec()
    print(f" {k_drain_safe.elapsed_s:.1f}s")

    print("  Pre-loading messages for Kafka (fast)...", end="", flush=True)
    _setup_kafka_dec()
    k_drain_fast = kafka_bench.run_batch_drain(fast=True)
    _teardown_kafka_dec()
    print(f" {k_drain_fast.elapsed_s:.1f}s")

    print("  Pre-loading messages for NATS...", end="", flush=True)
    _setup_nats_dec()
    n_drain = asyncio.run(nats_bench.run_batch_drain())
    _teardown_nats_dec()
    print(f" {n_drain.elapsed_s:.1f}s")

    results["batch_drain"] = {
        "kafka_safe": k_drain_safe.to_dict(),
        "kafka_fast": k_drain_fast.to_dict(),
        "nats": n_drain.to_dict(),
    }

    # ── 3. Saturation ─────────────────────────────────────────────────────────
    # (run saturation last — it's slow and modifies topic/stream mid-test)
    print("\n─── Saturation ramp (safe config) ───────────────────────────────")
    print("  Kafka...", end="", flush=True)
    k_sat = kafka_bench.run_saturation(fast=False)
    print(" done.")

    print("  NATS... ", end="", flush=True)
    n_sat = asyncio.run(nats_bench.run_saturation())
    print(" done.")

    results["saturation_safe"] = {
        "kafka": [vars(s) for s in k_sat],
        "nats": [vars(s) for s in n_sat],
    }

    # ── Print tables ──────────────────────────────────────────────────────────
    print_burst_table([dec_safe, dec_fast, wcm_safe])
    print_batch_drain_table(k_drain_safe, n_drain, k_drain_fast)
    print_saturation_table(k_sat, n_sat, fast=False)
    print_feature_table()

    path = save_results(results)
    print(f"\nResults saved → {path}\n")


if __name__ == "__main__":
    services.start_all()
    try:
        main()
    finally:
        services.stop_all()
