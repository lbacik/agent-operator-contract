# agent-operator-contract

The versioned operator contract between coding-agent engines (simple-coding-agent, coding-agent) and their consumers (agent-forge, agent-dashboard and the `agentfleet` client).

This repository owns the normative specification, its JSON Schemas, fixtures and conformance suites, a stateless shared library and the host-side `agentfleet` client. It depends on none of the engines or consumers. See [ADR 0001](docs/adr/0001-the-contract-lives-in-a-neutral-repository-and-is-negotiated-at-runtime.md).

- [Specification, major version 1](docs/spec.md) (final for `v1.0.0`)
- [JSON Schemas](src/agent_operator_contract/schemas/): normative for document shape; the spec is normative for semantics
- [Consumer fixtures and their classification index](src/agent_operator_contract/fixtures/agentctl/)
- [Conformance Harness examples](src/agent_operator_contract/fixtures/harness/) and [negative examples](src/agent_operator_contract/fixtures/negative/)
- [Stub producer](stub-producer/): the test-only engine the producer suite is proven against
- [Adoption plan](docs/adoption-plan.md) across the five repositories
- [Glossary](CONTEXT.md)
- [Decisions](docs/adr/)

## Versioning

One version line: the tag `vX.Y.Z` is the contract version and `X` is the contract major. The library, `agentfleet`, the schemas, the fixtures and both conformance suites are released together under that tag, and the package version equals it (`v1.0.0` ships `agent-operator-contract 1.0.0`). Engines and consumers depend on this repository as a git dependency pinned to a tag and run its fixtures and conformance suites in their own CI, without copying them.

## Pin the tag

The package is standard library only (`dependencies = []`), so pinning it costs nothing at runtime. Python 3.11 or newer.

```text
uv add "agent-operator-contract @ git+https://github.com/lbacik/agent-operator-contract@v1.0.2"
```

or, with pip:

```text
pip install "agent-operator-contract @ git+https://github.com/lbacik/agent-operator-contract@v1.0.2"
```

Schemas, fixtures and the error table ship as package data, so everything below works from the installed package.

## Use the library

- `agent_operator_contract.shared`: the producer side. `parse_argv` parses `agentctl` argv in the §4.1 check order `render_success` returns the success envelope as one line of JSON, and `render_error` returns the error envelope together with its §4 exit code.
- `agent_operator_contract.client`: the consumer side over the `docker` CLI. `discover` lists the Fleet from labels, `resolve` addresses one instance, `invoke` / `invoke_offline` run one operation (§3), `interpret` classifies the reply on `error.code`, `invoke_mutation_with_retry` retries ambiguous mutations with the same request ID.
- `agent_operator_contract.status`: typed Status Snapshot parsing, strict or lenient (unknown enum values become "unknown").
- `agent_operator_contract.documents`: stdlib validators that agree with the JSON Schemas, `load_schema`, and the fixture loaders.

```python
from agent_operator_contract import client, status

docker = client.docker_bin()
instance = client.resolve(client.discover(docker), "web-1")
envelope = client.interpret(client.invoke(docker, instance, ["status"]))
if envelope["ok"] is True:
    snapshot = status.parse_status_lenient(envelope["result"])
    print(snapshot.instance.state)
else:
    print(envelope["error"])  # decide on error["code"], never on the message
```

## Install `agentfleet`

`agentfleet` is the host-side client: `ls`, `identity`, `status`, every mutation, `command` and `attempts list|show|export`, online or as an Offline Read of a stopped instance.

```text
uv tool install "agent-operator-contract @ git+https://github.com/lbacik/agent-operator-contract@v1.0.2"
agentfleet --version
agentfleet ls
agentfleet status web-1
```

## Run the suites in CI

### Producer suite (engines)

An engine supplies a Conformance Harness (spec §15.1) and runs the suite from the pinned tag against its own image:

```yaml
- run: uv add --dev "agent-operator-contract @ git+https://github.com/lbacik/agent-operator-contract@v1.0.2"
- run: |
    uv run python -m agent_operator_contract.producer_suite \
      --harness ./conformance/harness \
      --capabilities stop-after,next-issue \
      --require-all
```

`--capabilities` names the Operator Capabilities the engine advertises; scenarios for the others do not run, and `unsupported-capability` checks they answer `unsupported`. `--require-all` fails the run when a scenario does not run. While an engine is still adopting, `--expect-fail <scenario-id>` (repeatable) tolerates a known failure and fails the run once that scenario passes. Output is one line per scenario plus a non-zero exit on failure.

### Consumer suite (consumers)

A consumer passes when it classifies every recorded fixture as the index says (spec §15). With pytest:

```python
import pytest
from agent_operator_contract import documents

INDEX = documents.load_fixture_index()["fixtures"]


@pytest.mark.parametrize("name", documents.fixture_names())
def test_classifies_fixture(name: str) -> None:
    fixture = documents.load_fixture(name)  # {argv, exit_code, stdout, stderr}
    assert my_consumer_classify(fixture) == INDEX[name]["classification"]
```

Replay the fixtures through whatever seam the consumer uses to reach `docker exec` (this repository's own consumer suite replays them through a fake `docker`, see [`tests/consumer/`](tests/consumer/)).

## Development

Requires [uv](https://docs.astral.sh/uv/). `uv sync` sets up the environment; `uv run pytest` runs the tests, including the release coverage gates in `tests/test_coverage_gates.py`. See [AGENTS.md](AGENTS.md#stack) for the stack conventions and the full check, and [`stub-producer/`](stub-producer/) to run the producer suite locally against the stub producer (needs Docker).
