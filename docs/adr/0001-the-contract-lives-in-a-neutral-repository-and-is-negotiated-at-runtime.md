---
status: accepted
---

# The contract lives in a neutral repository and is negotiated at runtime

The operator contract lives in this repository, which depends on no engine or consumer. It owns the normative specification, the JSON Schemas and fixtures, the conformance suite, a stateless shared library and the host-side `agentfleet` client. Engines own their lifecycle, command persistence, transport, redaction and export. Compatibility is negotiated at runtime, from the `--contract` major and the advertised Operator Capabilities. It is never inferred from the library version, the image or the backend name. Decided in [Where should the shared agent interface live, and what should it own?](https://github.com/lbacik/coding-agent/issues/191#issuecomment-6681592096).

## Considered options

- **A legacy facade over simple-coding-agent's `agentctl` text.** It would have frozen accidental dependencies: status prefixes, the `/data/logs` layout and an `"outcome"` cache heuristic.
- **simple-coding-agent or coding-agent as owner.** One engine would own the other's contract and invite accidental sharing of execution semantics.
- **agent-forge as owner.** Agents would depend on a skills repository, and contract releases would follow Forge's cadence.
- **agent-dashboard as owner.** A peer consumer would hand the contract to the others, and its releases would mix with a web application's.

## Consequences

- The reference engine loses contract ownership. Its examples seeded this repository's fixtures but are not a compatibility target.
- The contract normalizes only the in-container CLI over `docker exec`. This keeps simple-coding-agent's [ADR 0006](https://github.com/lbacik/simple-coding-agent/blob/38aaee14d4dd40d7b58c64ba6ffa5b5b6583dd66/docs/adr/0006-telemetry-through-agentctl-not-http.md): no port, no HTTP, no token. It deliberately departs from that ADR's "one connection at a time": `identity`, `status` and `command` must stay responsive while a mutation waits on the engine's lock.
