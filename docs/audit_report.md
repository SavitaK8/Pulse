# Pulse Security Audit & Code Review

## Overview
This document contains the findings of a security and architecture audit of the Pulse distributed rate limiter. Pulse is designed as a coordinator-free, in-memory rate limiter using a Bucketed G-Counter CRDT.

## 1. Security Findings

### 1.1 Unauthenticated Gossip Protocol (High Risk in Untrusted Networks)
- **Finding**: The `/gossip` endpoint accepts state updates from any source without authentication.
- **Impact**: An attacker with network access to the node can forge gossip messages, artificially inflating rate limits or injecting garbage keys to cause denial of service.
- **Mitigation**: Deploy Pulse in a trusted internal network (VPC/subnet) and configure firewalls to restrict access to the `/gossip` endpoint. Future iterations should add mutual TLS (mTLS) or payload signatures (HMAC) for peer authentication.

### 1.2 Resource Exhaustion via Key Space (Medium Risk)
- **Finding**: The `BucketedGCounter` tracks state per key. If the number of distinct keys exceeds `maxKeys` (default 100,000), new keys throw a `CapacityExceededException`, returning a 503 error for the `/check` endpoint.
- **Impact**: An attacker can perform a Denial of Service (DoS) by sending requests with random keys until the `maxKeys` limit is reached, blocking legitimate keys from being tracked.
- **Mitigation**: Implement a Least Recently Used (LRU) eviction policy for inactive keys, or use a consistent hashing ring to shard keys across different clusters to isolate abuse.

### 1.3 Missing HTTP Security Headers (Low Risk)
- **Finding**: The `HttpApi` returns JSON but does not include standard security headers like `X-Content-Type-Options: nosniff`.
- **Impact**: Minimal, as the API only returns JSON and does not render HTML, mitigating XSS risks.
- **Mitigation**: Add `X-Content-Type-Options: nosniff` to all HTTP responses.

### 1.4 Exception Handling (Positive Finding)
- **Finding**: The `HttpApi.internalError` method logs stack traces to `System.err` but returns a sanitized `{"error":"internal server error"}` to the client.
- **Impact**: Prevents information leakage of internal system details to external clients.

## 2. Architecture & Code Quality Findings

### 2.1 Concurrency Bottleneck
- **Finding**: `BucketedGCounter` uses a single `ReentrantReadWriteLock` for the entire state. Every `/check` call acquires a write lock.
- **Impact**: Under high concurrency, this single lock will become a bottleneck and limit the throughput of the node.
- **Recommendation**: Transition to a more granular locking mechanism, such as a `ConcurrentHashMap` combined with per-key locks or striped locks, to allow concurrent increments for different keys.

### 2.2 Time Drift Sensitivity
- **Finding**: Pulse heavily relies on `Math.floorDiv(clock.millis(), bucketMs)` to assign increments to time buckets.
- **Impact**: If node clocks are significantly out of sync, nodes might place counts in different buckets, breaking the CRDT convergence properties and leading to inaccurate rate limits.
- **Recommendation**: Document the strict requirement for NTP synchronization across all nodes.

### 2.3 Robust Input Validation
- **Finding**: `PulseConfig` provides thorough validation of environment variables. `HttpApi` limits the `/gossip` payload size and correctly guards against `Content-Length` spoofing.
- **Impact**: Excellent defense against malformed configuration and basic buffer overflow/memory exhaustion attacks.

## 3. Test Execution Summary
- **Local Environment Test Run**: Automated test execution via `run-tests.ps1` and `run-tests.sh` was attempted. 
- **Result**: The local environment lacks the necessary Java Development Kit (`javac`, `java`) and `docker` runtime. Tests could not be executed locally. 
- **Recommendation**: Ensure JDK 17+ or Docker is installed in the deployment and CI/CD environments. The test suite itself is well-structured, testing CRDT convergence, concurrent increments, and HTTP API integration.
