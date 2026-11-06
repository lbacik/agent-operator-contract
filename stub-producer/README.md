# Stub producer (test-only, never published)

A minimal engine that proves the producer suite, the harness protocol and the
shared part of the library are implementable before any real engine adopts
them. It is built as a Docker image in CI and never published.

- [`agentctl`](agentctl): the contract CLI at `/usr/local/bin/agentctl`. It
  uses the shared part of the library (`agent_operator_contract.shared`) to
  parse argv and render replies. It serves `identity`, `status`, the
  `command` read and `attempts list` / `attempts show` plus the core
  mutations `stop` and `resume` (and a minimal `stop --after` plan) with
  durable Intake, Claims and Attempts on its state volume. The engine opens a
  Claim for an eligible issue behind a Claim boundary, runs recovery at start,
  records Halts durably, and honors harness holds (notably `before-claim`)
  and one-shot crashes armed by the harness; anything else well-formed
  answers `internal`. Mutations commit while holding the engine lock file;
  `status` never waits on it and instead serves the last prepared snapshot
  while the lock is held. Snapshots are bounded to 256 KiB with oldest-first
  truncation (events, then figures, then commands).
- [`harness`](harness): the Conformance Harness executable. It implements the
  protocol verbs by steering a stub container over the Docker CLI: `setup`,
  `teardown`, `set-issue` (eligible/ineligible), `hold`/`release` at named
  points, `finish-attempt` with a native outcome, `crash` at every crash
  point, `terminate` (SIGTERM), `stop-container`/`start-container`, `halt`
  with a reason, `grow-archive`, `hold-lock` (holds the engine lock file
  for the requested duration) and `defer-publication`
  (leaves a publication effect pending at restart). State mutations run
  inside the container over `docker exec`, so the stub engine honors holds,
  crashes, Halts and deferred publication across restarts.
- [`Dockerfile`](Dockerfile): the test-only image with a non-root `agent`
  user and the shared library on `PYTHONPATH`.

Build and run the suite (needs Docker):

```text
docker build -f stub-producer/Dockerfile -t stub-producer:ci .
STUB_PRODUCER_IMAGE=stub-producer:ci \
  uv run python -m agent_operator_contract.producer_suite \
    --harness ./stub-producer/harness
```

`STUB_PRODUCER_IMAGE` selects the image the harness starts;
`STUB_HARNESS_STATE` selects where the harness tracks its instance;
`DOCKER` selects the Docker executable.
