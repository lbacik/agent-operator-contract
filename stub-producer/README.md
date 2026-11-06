# Stub producer (test-only, never published)

A minimal engine that proves the producer suite, the harness protocol and the
shared part of the library are implementable before any real engine adopts
them. It is built as a Docker image in CI and never published.

- [`agentctl`](agentctl): the contract CLI at `/usr/local/bin/agentctl`. It
  uses the shared part of the library (`agent_operator_contract.shared`) to
  parse argv and render replies. It serves `identity` and the grammar
  failures; anything else well-formed answers `internal` until later issues
  implement the full core.
- [`harness`](harness): the Conformance Harness executable. It implements
  `setup` and `teardown` by steering a stub container over the Docker CLI
  and answers `unsupported` for every other verb, so dependent scenarios are
  reported as not run.
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
