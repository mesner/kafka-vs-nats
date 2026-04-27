import time
import statistics
from dataclasses import dataclass, field


@dataclass
class ScenarioResult:
    scenario: str
    system: str
    msg_count: int
    total_bytes: int
    elapsed_s: float
    latencies_ns: list[int] = field(default_factory=list)
    throughput_msg_s: float = 0.0
    throughput_mb_s: float = 0.0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    pmax_ms: float = 0.0

    def finalize(self) -> "ScenarioResult":
        if self.elapsed_s > 0:
            self.throughput_msg_s = self.msg_count / self.elapsed_s
            self.throughput_mb_s = (self.total_bytes / 1_048_576) / self.elapsed_s
        if self.latencies_ns:
            ns = sorted(self.latencies_ns)
            self.p50_ms = statistics.median(ns) / 1_000_000
            n = len(ns)
            self.p95_ms = ns[int(n * 0.95)] / 1_000_000
            self.p99_ms = ns[int(n * 0.99)] / 1_000_000
            self.pmax_ms = ns[-1] / 1_000_000
        return self

    def to_dict(self) -> dict:
        return {
            "scenario": self.scenario,
            "system": self.system,
            "msg_count": self.msg_count,
            "total_bytes": self.total_bytes,
            "elapsed_s": round(self.elapsed_s, 3),
            "throughput_msg_s": round(self.throughput_msg_s, 2),
            "throughput_mb_s": round(self.throughput_mb_s, 3),
            "p50_ms": round(self.p50_ms, 3),
            "p95_ms": round(self.p95_ms, 3),
            "p99_ms": round(self.p99_ms, 3),
            "pmax_ms": round(self.pmax_ms, 3),
        }


class MetricsCollector:
    def __init__(self, scenario: str, system: str):
        self._scenario = scenario
        self._system = system
        self._latencies: list[int] = []
        self._total_bytes = 0
        self._start_ns = 0
        self._end_ns = 0

    def start(self) -> None:
        self._start_ns = time.perf_counter_ns()

    def stop(self) -> None:
        self._end_ns = time.perf_counter_ns()

    def record_recv(self, payload: bytes) -> None:
        now = time.perf_counter_ns()
        sent_ns = int(payload[:20])
        self._latencies.append(now - sent_ns)
        self._total_bytes += len(payload)

    def result(self) -> ScenarioResult:
        elapsed = (self._end_ns - self._start_ns) / 1e9
        return ScenarioResult(
            scenario=self._scenario,
            system=self._system,
            msg_count=len(self._latencies),
            total_bytes=self._total_bytes,
            elapsed_s=elapsed,
            latencies_ns=self._latencies,
        ).finalize()


def stamp(payload: str | bytes) -> bytes:
    prefix = f"{time.perf_counter_ns():020d}".encode()
    if isinstance(payload, str):
        return prefix + payload.encode()
    return prefix + payload
