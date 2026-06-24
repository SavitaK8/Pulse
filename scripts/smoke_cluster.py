#!/usr/bin/env python3
"""Compile, launch, and smoke-test a local three-node Pulse cluster."""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid


ROOT = pathlib.Path(__file__).resolve().parent.parent
NODES = [
    ("node-a", 9001, "localhost:9002,localhost:9003"),
    ("node-b", 9002, "localhost:9001,localhost:9003"),
    ("node-c", 9003, "localhost:9001,localhost:9002"),
]


def get_json(port: int, path: str, key: str | None = None) -> dict:
    query = "" if key is None else "?" + urllib.parse.urlencode({"key": key})
    with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/{path}{query}", timeout=2) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_healthy() -> None:
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            if all(get_json(port, "health")["status"] == "ok"
                   for _, port, _ in NODES):
                return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("cluster did not become healthy")


def main() -> None:
    work_dir = ROOT / "work"
    work_dir.mkdir(exist_ok=True)
    classes_dir = work_dir / "classes"
    classes_dir.mkdir(exist_ok=True)
    java_files = [str(path) for path in ROOT.glob("*.java")]
    subprocess.run(["javac", "-encoding", "UTF-8", "-d", str(classes_dir), *java_files],
                   cwd=ROOT, check=True)
    processes = []
    logs = []
    try:
        for node_id, port, peers in NODES:
            environment = os.environ.copy()
            environment.update({
                "NODE_ID": node_id,
                "PORT": str(port),
                "PEERS": peers,
                "LIMIT": "50",
                "WINDOW_BUCKETS": "10",
                "BUCKET_MS": "1000",
                "GOSSIP_INTERVAL_MS": "50",
            })
            log = open(work_dir / f"{node_id}.log", "w", encoding="utf-8")
            logs.append(log)
            processes.append(subprocess.Popen(
                ["java", "-cp", str(classes_dir), "PulseNode"],
                cwd=ROOT,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT))

        wait_healthy()
        key = f"smoke-{uuid.uuid4().hex}"
        for _, port, _ in NODES:
            for _ in range(20):
                get_json(port, "check", key)

        deadline = time.time() + 5
        counts = []
        while time.time() < deadline:
            counts = [get_json(port, "estimate", key)["estimatedCount"]
                      for _, port, _ in NODES]
            if counts == [60, 60, 60]:
                break
            time.sleep(0.05)
        if counts != [60, 60, 60]:
            raise AssertionError(f"cluster failed to converge: {counts}")
        if get_json(9001, "check", key)["allowed"]:
            raise AssertionError("node-a allowed a request above the converged limit")
        print("PASS three-node convergence:", counts)

        benchmark = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "benchmark.py"),
                "--nodes",
                "http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003",
                "load",
                "--requests",
                "30",
                "--concurrency",
                "6",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True)
        benchmark_result = json.loads(benchmark.stdout)
        if benchmark_result["failures"] != 0 or benchmark_result["requests"] != 30:
            raise AssertionError(f"benchmark smoke failed: {benchmark_result}")
        print("PASS benchmark client against live cluster")

        processes[1].terminate()
        processes[1].wait(timeout=5)
        partition_key = f"partition-{uuid.uuid4().hex}"
        if not get_json(9001, "check", partition_key)["allowed"]:
            raise AssertionError("node-a stopped serving during peer failure")
        if not get_json(9003, "check", partition_key)["allowed"]:
            raise AssertionError("node-c stopped serving during peer failure")
        print("PASS remaining nodes serve while node-b is down")
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        for log in logs:
            log.close()


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as exception:
        print(f"Missing required executable: {exception.filename}", file=sys.stderr)
        raise SystemExit(2)
