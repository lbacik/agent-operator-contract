# Consumer fixtures (examples)

Each file records one `agentctl` invocation as `{argv, exit_code, stdout, stderr}`. `argv` excludes `/usr/local/bin/agentctl` itself. `stdout` is the exact bytes a producer writes: one JSON document and a newline.

These examples illustrate [the specification](../../docs/spec.md); they are not yet the full consumer suite. Adoption step 1 extends them to every exit code and `error.code` and validates them against the JSON Schemas. Consumers classify each one: success, replay, rejection, `unsupported`, contract mismatch, not found, or ambiguous outcome (`agent-unreachable.json`, which must be retried with the same request ID).
