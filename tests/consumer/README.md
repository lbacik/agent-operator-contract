# Consumer suite: fake `docker` and Fleet description

The consumer boundary is the `docker` executable: `agentfleet` and the
library's client part talk to Docker only through the configurable `docker`
CLI via subprocess. This directory holds the replaying fake that stands in
for `docker` in tests, so the suite needs no real daemon, no network and no
running agent container.

## Layout

- `fake_docker`: the fake executable (stdlib only, `chmod +x`). It serves a
  described Fleet for listing and inspection, answers `exec` of `agentctl`
  by replaying fixtures matched on argv, and records every invocation.
- `example-fleet.json`: an example Fleet description with a duplicate name
  and a Compose one-off container.
- `../test_consumer.py`: the suite that runs `agentfleet` against the fake.

## Fleet description

A JSON file `{"containers": [...]}` where each container carries the four
discovery labels plus container state:

```json
{
  "containers": [
    {"id": "abc123", "name": "web-1", "backend": "coding-agent",
     "contract": "1", "exec_user": "agent", "state": "running"},
    {"id": "def456", "name": "web-1", "backend": "coding-agent",
     "contract": "1", "exec_user": "agent", "state": "exited"},
    {"id": "oneoff9", "name": "worker-run-1", "backend": "coding-agent",
     "contract": "1", "exec_user": "agent", "state": "running", "oneoff": true}
  ]
}
```

`oneoff: true` sets `com.docker.compose.oneoff=True`, which the consumer
drops even when labelled. Two containers with the same `name` model a
duplicate: `agentfleet ls` flags them, `agentfleet identity <dup>` refuses
without any `exec`.

## Fixtures

The replayed producer answers live in
`src/agent_operator_contract/fixtures/agentctl/` as plain JSON files
`{argv, exit_code, stdout, stderr}` (`argv` excludes the `agentctl` binary
itself). The fake matches the args after `/usr/local/bin/agentctl` against
`argv` and replays `stdout`/`stderr` with `exit_code`, byte for byte. No
Python import is needed to use them: they are files on disk in the checkout.

## Invocation from a checkout of the tag

No installation and no Python imports are required: the fake runs with the
system Python 3.11+ and reads the Fleet and fixture files from disk.

```sh
# From the repository root at the pinned tag:
export FAKE_DOCKER_FLEET=tests/consumer/example-fleet.json
export FAKE_DOCKER_LOG=/tmp/fake-docker.log
export AGENTFLEET_DOCKER=tests/consumer/fake_docker
# Fixtures default to the checkout's src/.../fixtures/agentctl/;
# override with FAKE_DOCKER_FIXTURES=/path/to/fixtures.

: > /tmp/fake-docker.log
AGENTFLEET_DOCKER="$PWD/tests/consumer/fake_docker" \
FAKE_DOCKER_FLEET="$PWD/tests/consumer/example-fleet.json" \
FAKE_DOCKER_LOG=/tmp/fake-docker.log \
  python3 -m agent_operator_contract.agentfleet ls

AGENTFLEET_DOCKER="$PWD/tests/consumer/fake_docker" \
FAKE_DOCKER_FLEET="$PWD/tests/consumer/example-fleet.json" \
FAKE_DOCKER_LOG=/tmp/fake-docker.log \
  python3 -m agent_operator_contract.agentfleet identity web-1 --json

cat /tmp/fake-docker.log  # one {"argv": [...]} line per docker invocation
```

Point `AGENTFLEET_DOCKER` at the fake (or put its directory on `PATH` as
`docker`); `--docker` on the `agentfleet` command line overrides both.
