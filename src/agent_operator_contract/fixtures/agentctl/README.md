# Consumer fixtures

Each file records one `agentctl` invocation as `{argv, exit_code, stdout, stderr}`. `argv` excludes `/usr/local/bin/agentctl` itself. `stdout` is the exact bytes a producer writes: one JSON document and a newline (the `malformed_stdout` fixtures break that rule on purpose).

Together with `index.json` these files are the consumer suite of [spec section 15](../../../../docs/spec.md#15-conformance). They ship as package data, so a consumer that pins this repository as a git dependency loads them from the installed package without copying them:

```python
from importlib import resources

package = resources.files("agent_operator_contract")
fixture = package / "fixtures" / "agentctl" / "identity.json"
```

or through the helpers in `agent_operator_contract.documents` (`fixture_names`, `load_fixture`, `load_fixture_index`, `classify_fixture`).

## Classification index

`index.json` records each fixture's expected consumer classification and a one-line note, so the recording files themselves stay byte-exact:

```json
{"version": 1,
 "classifications": ["success", "replay", "..."],
 "fixtures": {"identity": {"classification": "success", "note": "Exit 0, ok:true with the Identity result."}}}
```

| Classification | Meaning |
| --- | --- |
| `success` | Exit 0, `ok:true`, and the effect (if any) committed. |
| `replay` | Exit 0, `ok:true`, `result.replayed:true`: the request ID already existed with the identical payload and nothing new happened. |
| `rejection` | A refused call, exit 1 (`rejected`, `repository_mismatch`, `payload_mismatch`, `invalid_argument`), exit 2 (`usage`) or exit 7 with `details.stored:false`. Nothing stored. |
| `unsupported` | Exit 3: the operation or capability is not advertised. |
| `contract_mismatch` | Exit 4: the consumer's `--contract` major is not the producer's. |
| `not_found` | Exit 5: unknown request ID, Attempt or archive path. |
| `ambiguous_outcome` | Exit 6 or 7 (`agent_unreachable`, or `internal` without `details.stored:false`): the mutation outcome is unknown and must be retried with the same request ID. |
| `malformed_stdout` | The stdout is not a valid envelope, or the exit code contradicts it: the consumer cannot trust this answer. Docker's own exits 125 to 127 land here as transport failures. |

A consumer passes the consumer suite when it classifies every fixture as `index.json` says. `documents.classify_fixture` implements this reading; tests require it to agree with `index.json` on every fixture, and `index.json` to list every fixture and nothing else.

## The fixture set

| Area | Fixtures |
| --- | --- |
| Identity | `identity` |
| Status Snapshot, one per `instance.state` | `status-starting`, `status-idle`, `status-active`, `status-draining`, `status-paused`, `status-recovering`, `offline-status-halted` (`stopped`), plus `status-simple-coding-agent` for the second backend's shape |
| `command` read | `command-found`, `command-not-found` |
| Core mutations | `stop-accepted`, `stop-replayed`, `resume-accepted` |
| Capability mutations | `stop-after-accepted`, `next-issue-accepted`, `handoff-completed`, `handoff-rejected`, `recovery-retry-completed`, `recovery-retry-not-found`, `recovery-release-completed`, `recovery-release-not-found`, `errors-reset-completed`, `errors-reset-rejected` |
| Attempt Archives | `attempts-list`, `attempts-list-paged`, `attempts-show-sealed`, `attempts-show-unsealed`, `attempts-not-found`, `attempts-export-not-found` |
| Offline Read | `offline-status-halted`, `offline-identity-running` (`rejected` / `instance_running`) |
| Rejections | `rejected`, `repository-mismatch`, `payload-mismatch`, `invalid-argument`, `invalid-argument-after-0`, `usage`, `internal-not-stored` |
| Unsupported and version | `unsupported`, `contract-mismatch` |
| Ambiguous outcomes | `agent-unreachable`, `internal` |
| Malformed and transport | `malformed-empty`, `malformed-non-json`, `malformed-truncated`, `contract-unknown-major`, `transport-125`, `transport-126`, `transport-127` |

## Coverage gates

`tests/test_coverage_gates.py` fails the build when the spec outgrows this set: every `error.code` and every exit code of spec section 4 (0 to 7, and Docker's 125 to 127) appears in at least one fixture, and every `instance.state` has a Status Snapshot fixture. Every fixture validates against `schemas/fixture.schema.json`, and every envelope in it against the JSON Schemas.

Sibling directories hold the other example sets: [`../harness/`](../harness/) for Conformance Harness requests and replies, and [`../negative/`](../negative/) for documents that every schema and validator must reject.
