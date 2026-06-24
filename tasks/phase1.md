# Phase 1 Checklist

## Blueprint

- [x] Define goals and non-goals.
- [x] Define environment configuration.
- [x] Define API contracts.
- [x] Define module boundaries.
- [x] Record distributed-system trade-offs.

## Core build

- [x] Implement validated configuration.
- [x] Implement thread-safe bucketed G-Counter.
- [x] Implement local rate-limit service.
- [x] Implement versioned gossip codec.
- [x] Implement periodic peer gossip.
- [x] Implement HTTP API and lifecycle.
- [x] Implement metrics.

## Quality

- [x] Test CRDT convergence and idempotence.
- [x] Test bucket expiry.
- [x] Test local admission behavior.
- [x] Test gossip codec round trip and malformed input.
- [x] Test HTTP endpoints using an ephemeral local port.
- [x] Add a cluster smoke test.
- [x] Add benchmark scripts and expected output schema.

## Operations

- [x] Add Dockerfile and three-node Compose file.
- [x] Add local and Docker run instructions.
- [x] Document restart identity and clock-skew risks.
- [x] Explain end-to-end data flow.
- [x] Perform final architecture self-review.
