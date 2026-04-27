import subprocess
import time
import socket
import os
import signal
import asyncio
import tempfile

KAFKA_START = "/opt/homebrew/opt/kafka/bin/kafka-server-start"
KAFKA_CONFIG = "/opt/homebrew/etc/kafka/server.properties"
NATS_START = "/opt/homebrew/opt/nats-server/bin/nats-server"

_procs: dict[str, subprocess.Popen] = {}
_nats_store_dir: str | None = None


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _wait_for_port(host: str, port: int, label: str, timeout: int = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _port_open(host, port):
            return
        time.sleep(0.5)
    raise TimeoutError(f"{label} did not become available on {host}:{port} within {timeout}s")


def start_kafka(log_file: str = "/tmp/kafka-bench.log") -> None:
    if "kafka" in _procs and _procs["kafka"].poll() is None:
        return
    with open(log_file, "w") as lf:
        proc = subprocess.Popen(
            [KAFKA_START, KAFKA_CONFIG],
            stdout=lf,
            stderr=lf,
            preexec_fn=os.setsid,
        )
    _procs["kafka"] = proc
    print(f"  Starting Kafka (pid {proc.pid})...", end="", flush=True)
    _wait_for_port("localhost", 9092, "Kafka")
    print(" ready.")


def stop_kafka() -> None:
    proc = _procs.pop("kafka", None)
    if proc is None or proc.poll() is not None:
        return
    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)


def start_nats(log_file: str = "/tmp/nats-bench.log") -> None:
    global _nats_store_dir
    if "nats" in _procs and _procs["nats"].poll() is None:
        return
    _nats_store_dir = tempfile.mkdtemp(prefix="nats-bench-")
    with open(log_file, "w") as lf:
        proc = subprocess.Popen(
            [NATS_START, "--jetstream", "--store_dir", _nats_store_dir, "--port", "4222"],
            stdout=lf,
            stderr=lf,
            preexec_fn=os.setsid,
        )
    _procs["nats"] = proc
    print(f"  Starting NATS/JetStream (pid {proc.pid})...", end="", flush=True)
    _wait_for_port("localhost", 4222, "NATS")
    print(" ready.")


def stop_nats() -> None:
    global _nats_store_dir
    proc = _procs.pop("nats", None)
    if proc is None or proc.poll() is not None:
        return
    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    if _nats_store_dir and os.path.exists(_nats_store_dir):
        import shutil
        shutil.rmtree(_nats_store_dir, ignore_errors=True)
        _nats_store_dir = None


def start_all() -> None:
    print("Starting services...")
    start_kafka()
    start_nats()


def stop_all() -> None:
    print("\nStopping services...")
    stop_nats()
    stop_kafka()
