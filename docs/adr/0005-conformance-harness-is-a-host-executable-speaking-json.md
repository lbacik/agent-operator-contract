---
status: accepted
---

# The conformance harness is a host-side executable speaking JSON

Each engine's Conformance Harness is an executable that runs on the host, outside the production image. The producer suite invokes it as `<harness> <verb>` with a JSON document as the argument and reads one JSON reply from stdout, in the same style as `agentctl`: fourteen verbs from fresh `setup` to `defer-publication`, three error codes (`failed`, `usage`, `unsupported`), and an `unsupported` reply that names what the engine cannot do so the suite reports dependent scenarios as not run, never as passed. The request and reply shapes ship as Draft 2020-12 schemas with positive and negative examples, checked in CI like the other documents. Decided in [Settle the Conformance Harness protocol](https://github.com/lbacik/agent-operator-contract/issues/4).

## Why

- **No engine imports this repository's code.** A harness in any language builds fourteen verbs over JSON; there is nothing to pin, vendor or keep in step with this repository's releases. The engines stay free to be written in whatever language suits them.
- **The same shape as the contract it tests.** Verbs in argv, one JSON document on stdout, free-form stderr, classified exit codes: every recording, fixture and schema-checking tool built for `agentctl` transfers to the harness with no new ideas.
- **Black-box steering keeps producer internals private.** The suite never touches the engine's fakes, container or volumes directly; everything crosses one narrow interface whose every message is validatable. A `failed` or `usage` answer therefore indicts the harness or the suite, never the engine's hidden state.
- **Unsupported is data, not silence.** Engines honestly differ: synchronous publication cannot defer, some lifecycles cannot pause after the outcome. Naming the unsupported `what` and `name` lets the suite skip exactly the scenarios that need them instead of guessing from a crash.

## Considered options

- **A Python interface engines implement.** The suite would import an engine-supplied module and call its functions. It forces a Python runtime and this repository's import surface on every engine team, couples harness releases to this repository's cadence, lets in-process failures masquerade as engine behavior, and cannot be recorded or replayed as text. It was rejected: the runtime of this repository is deliberately stdlib-only so engines can pin it at no cost, not so engines must run it.
- **An HTTP or gRPC control API on the harness.** It buys streaming and typing at the price of ports, schemas-as-code and client libraries per engine language, for an interface whose busiest verb blocks on a timer and whose largest reply fits in a line. A process boundary with JSON documents is enough, and it works in CI without network services.
- **Driving the fakes directly from the suite.** The suite would learn each engine's fake provider and fake GitHub. That leaks every engine's test doubles into the shared suite and makes each new engine rewrite it. The harness keeps the fakes behind the engine's own boundary.

## Consequences

- Engine teams implement fourteen verbs and three error codes against the schemas in this repository, in any language, with no import of its code.
- The suite and the harness share a tag: requests are strict (unknown parameters are `usage`) so suite typos fail loudly, while replies are lenient (unknown `result` and `details` keys are ignored) so a harness minor can add fields.
- Hold points, crash points, native outcomes and capabilities the engine lacks are answered `unsupported` with `details.what` and `details.name`; the suite must track those answers per scenario and report not run, never pass.
- The harness manages one current instance at a time: `setup` then `teardown`, in that order, per scenario.
