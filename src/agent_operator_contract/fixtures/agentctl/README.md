# Consumer fixtures

Each file records one `agentctl` invocation as `{argv, exit_code, stdout, stderr}`. `argv` excludes `/usr/local/bin/agentctl` itself. `stdout` is the exact bytes a producer writes: one JSON document and a newline.

These files are the seed of the consumer suite (spec section 15). They ship as package data, so a consumer that pins this repository as a git dependency loads them from the installed package without copying them:

```python
from importlib import resources

package = resources.files("agent_operator_contract")
fixture = package / "fixtures" / "agentctl" / "identity.json"
```

or through the helpers in `agent_operator_contract.documents`
(`fixture_names`, `load_fixture`, `load_fixture_index`).

## Classification index

`index.json` records each fixture's expected consumer classification, so the recording files themselves stay byte-exact:

| Classification | Meaning |
| --- | --- |
| `success` | Exit 0, `ok:true`, and the effect (if any) committed. |
| `replay` | Exit 0, `ok:true`, `result.replayed:true`: the request ID already existed with the identical payload and nothing new happened. |
| `rejection` | A refused call, exit 1 (`rejected`, `repository_mismatch`, `payload_mismatch`, `invalid_argument`) or exit 2 (`usage`). Nothing stored. |
| `unsupported` | Exit 3: the operation or capability is not advertised. |
| `contract_mismatch` | Exit 4: the consumer's `--contract` major is not the producer's. |
| `not_found` | Exit 5: unknown request ID, Attempt or archive path. |
| `ambiguous_outcome` | Exit 6 or 7 (`agent_unreachable`, or `internal` without `details.stored:false`): the mutation outcome is unknown and must be retried with the same request ID. |
| `malformed_stdout` | The stdout is not a valid envelope, or the exit code contradicts it: the consumer cannot trust this answer. |

`documents.classify_fixture` implements this reading; tests require it to agree with `index.json` on every fixture.

These examples illustrate [the specification](../../../../docs/spec.md) and seed the consumer suite, covering every exit code and `error.code` family the contract defines.
