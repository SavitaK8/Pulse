# Project Context

## Current objective

Build Pulse, a coordinator-free distributed rate limiter in dependency-free
Java, including a runnable three-node Docker cluster, tests, metrics, and
benchmark tooling.

## Working agreements

- Architecture and contracts are written before implementation.
- Production modules stay small and single-purpose.
- Tests are added with the feature, not postponed.
- Session state and important reasoning live in repository documents.
- Each phase ends with an end-to-end flow and architecture self-review.
- Avoid patch piles: refactor when boundaries become unclear.

## Current phase

Phase 1 implementation is complete. Unit/HTTP tests and the three-node
convergence/partition smoke test pass. Docker files are present but Docker was
not installed in the build environment, so the image itself was not executed
there.

## Key constraints

- Local environment currently has no `java`, `javac`, or `docker` command on
  `PATH`, so compilation/runtime verification may require another machine or a
  later tool installation.
- The application must still use only JDK standard-library APIs.
- The workspace was empty when work began and is not a Git repository.
- Android Studio's bundled JDK was used for local Java verification.
- Codex's bundled Python runtime was used for script syntax checks and the
  cluster smoke test.
- The benchmark client was exercised against the live local three-node cluster
  as part of the smoke test.

## Decisions to remember

- Fixed epoch buckets; active window is `WINDOW_BUCKETS × BUCKET_MS`.
- Per-cell merge is maximum.
- `/check` has no peer/network dependency.
- Full-state gossip is intentionally the first implementation.
- Gossip is trusted-network traffic and needs authentication before production.
