# Harness examples

One file per example, named `<verb>.request.json` (the JSON argument of one
harness invocation) or `<verb>.reply.json` (the JSON document the harness
writes to stdout for it). Extra files carry a suffix after the verb
(`hold-native-phase.request.json`, `hold-engaged.reply.json`,
`error-unsupported-hold-point.reply.json`).

These files are the seed of the producer suite (spec section 15.1). They ship
as package data, so a producer suite that pins this repository as a git
dependency loads them from the installed package without copying them:

```python
from agent_operator_contract import documents

request = documents.load_harness_example("setup.request")
reply = documents.load_harness_example("setup.reply")
```

or through the helpers in `agent_operator_contract.documents`
(`harness_example_names`, `load_harness_example`).

## Request and reply

The invocation is `<harness> <verb> '<request-json>'`, in the same style as
`agentctl`: the verb travels in argv and again inside the JSON document, and
the two must agree. Every invocation writes exactly one JSON document and a
newline to stdout. `ok:true` replies carry `result`; `ok:false` replies carry
`error` with one of three codes:

| `error.code` | Exit | Meaning |
| --- | --- | --- |
| `failed` | 1 | The harness could not perform the action. |
| `usage` | 2 | Malformed invocation: unknown verb, verb mismatch, bad JSON, invalid parameters. |
| `unsupported` | 3 | The engine cannot do this. `details` names the `what` (`verb`, `hold-point`, `crash-point`, `native-outcome`, `capability`) and the `name`. The suite reports dependent scenarios as not run, never as passed. |

Requests are strict (unknown parameters are rejected, so suite typos fail
loudly); replies are lenient (unknown `result` and `details` keys are
allowed, so a minor can add fields).
