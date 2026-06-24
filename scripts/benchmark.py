#!/usr/bin/env python3
"""Dependency-free load, convergence, and gossip-bandwidth measurements."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import statistics
import time
import urllib.parse
import urllib.request
import uuid


def request_json(url: str, timeout: float = 3.0) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def endpoint(node: str, path: str, key: str) -> str:
    return f"{node.rstrip('/')}/{path}?{urllib.parse.urlencode({'key': key})}"


def one_check(node: str, key: str) -> tuple[float, dict]:
    started = time.perf_counter_ns()
    body = request_json(endpoint(node, "check", key))
    elapsed_us = (time.perf_counter_ns() - started) / 1_000
    return elapsed_us, body


def percentile(samples: list[float], fraction: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * fraction) - 1))
    return ordered[index]


def send_load(nodes: list[str], key: str, requests: int, concurrency: int) -> dict:
    started = time.perf_counter()
    latencies: list[float] = []
    allowed = 0
    denied = 0
    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(one_check, nodes[index % len(nodes)], key)
            for index in range(requests)
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                latency, body = future.result()
                latencies.append(latency)
                if body["allowed"]:
                    allowed += 1
                else:
                    denied += 1
            except Exception:
                failures += 1
    elapsed = time.perf_counter() - started
    return {
        "key": key,
        "requests": requests,
        "allowed": allowed,
        "denied": denied,
        "failures": failures,
        "elapsed_seconds": round(elapsed, 6),
        "requests_per_second": round((requests - failures) / elapsed, 2),
        "latency_us": {
            "p50": round(percentile(latencies, 0.50), 2),
            "p95": round(percentile(latencies, 0.95), 2),
            "p99": round(percentile(latencies, 0.99), 2),
            "mean": round(statistics.fmean(latencies), 2) if latencies else 0,
        },
    }


def estimates(nodes: list[str], key: str) -> list[dict]:
    return [request_json(endpoint(node, "estimate", key)) for node in nodes]


def convergence(args: argparse.Namespace) -> None:
    key = args.key or f"pulse-convergence-{uuid.uuid4().hex}"
    load = send_load(args.nodes, key, args.requests, args.concurrency)
    timeline = []
    started = time.perf_counter()
    deadline = started + args.timeout_seconds
    converged = False
    while time.perf_counter() < deadline:
        current = estimates(args.nodes, key)
        elapsed_ms = round((time.perf_counter() - started) * 1_000, 2)
        counts = [item["estimatedCount"] for item in current]
        timeline.append({"elapsed_ms": elapsed_ms, "counts": counts})
        if len(set(counts)) == 1 and counts[0] == args.requests:
            converged = True
            break
        time.sleep(args.poll_ms / 1_000)
    print(json.dumps({
        "mode": "convergence",
        "nodes": args.nodes,
        "load": load,
        "over_admission": max(0, load["allowed"] - args.limit),
        "converged": converged,
        "convergence_ms": timeline[-1]["elapsed_ms"] if converged else None,
        "timeline": timeline,
    }, indent=2))


def load(args: argparse.Namespace) -> None:
    key = args.key or f"pulse-load-{uuid.uuid4().hex}"
    print(json.dumps({
        "mode": "load",
        "nodes": args.nodes,
        **send_load(args.nodes, key, args.requests, args.concurrency),
    }, indent=2))


def parse_prometheus(node: str) -> dict[str, float]:
    with urllib.request.urlopen(f"{node.rstrip('/')}/metrics", timeout=3) as response:
        lines = response.read().decode("utf-8").splitlines()
    result = {}
    for line in lines:
        if not line or line.startswith("#"):
            continue
        name, value = line.split(maxsplit=1)
        result[name] = float(value)
    return result


def bandwidth(args: argparse.Namespace) -> None:
    before = [parse_prometheus(node) for node in args.nodes]
    prefix = args.key or f"pulse-bandwidth-{uuid.uuid4().hex}"
    for index in range(args.keys):
        one_check(args.nodes[index % len(args.nodes)], f"{prefix}-{index}")
    time.sleep(args.settle_seconds)
    after = [parse_prometheus(node) for node in args.nodes]
    node_results = []
    for node, start, end in zip(args.nodes, before, after):
        sent = end.get("pulse_gossip_sent_total", 0) - start.get(
            "pulse_gossip_sent_total", 0)
        byte_count = end.get("pulse_gossip_bytes_sent_total", 0) - start.get(
            "pulse_gossip_bytes_sent_total", 0)
        node_results.append({
            "node": node,
            "messages_sent": int(sent),
            "bytes_sent": int(byte_count),
            "average_payload_bytes": round(byte_count / sent, 2) if sent else None,
        })
    print(json.dumps({
        "mode": "bandwidth",
        "distinct_keys_added": args.keys,
        "settle_seconds": args.settle_seconds,
        "nodes": node_results,
    }, indent=2))


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    root.add_argument(
        "--nodes",
        default="http://localhost:8081,http://localhost:8082,http://localhost:8083",
        help="comma-separated node base URLs")
    subcommands = root.add_subparsers(dest="mode", required=True)

    load_parser = subcommands.add_parser("load")
    load_parser.add_argument("--requests", type=int, default=10_000)
    load_parser.add_argument("--concurrency", type=int, default=100)
    load_parser.add_argument("--key")
    load_parser.set_defaults(run=load)

    convergence_parser = subcommands.add_parser("convergence")
    convergence_parser.add_argument("--requests", type=int, default=60)
    convergence_parser.add_argument("--concurrency", type=int, default=30)
    convergence_parser.add_argument("--limit", type=int, default=50)
    convergence_parser.add_argument("--poll-ms", type=int, default=20)
    convergence_parser.add_argument("--timeout-seconds", type=float, default=5)
    convergence_parser.add_argument("--key")
    convergence_parser.set_defaults(run=convergence)

    bandwidth_parser = subcommands.add_parser("bandwidth")
    bandwidth_parser.add_argument("--keys", type=int, default=1_000)
    bandwidth_parser.add_argument("--settle-seconds", type=float, default=2)
    bandwidth_parser.add_argument("--key")
    bandwidth_parser.set_defaults(run=bandwidth)
    return root


if __name__ == "__main__":
    arguments = parser().parse_args()
    arguments.nodes = [node.strip() for node in arguments.nodes.split(",")
                       if node.strip()]
    if not arguments.nodes:
        raise SystemExit("--nodes must contain at least one URL")
    arguments.run(arguments)
