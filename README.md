# agent-operator-contract

The versioned operator contract between coding-agent engines (simple-coding-agent, coding-agent) and their consumers (agent-forge, agent-dashboard and the `agentfleet` client).

This repository owns the normative specification, its fixtures and conformance suite, a stateless shared library and the host-side `agentfleet` client. It depends on none of the engines or consumers. See [ADR 0001](docs/adr/0001-the-contract-lives-in-a-neutral-repository-and-is-negotiated-at-runtime.md).

- [Specification, major version 1](docs/spec.md) (draft for `v1.0.0`)
- [Adoption plan](docs/adoption-plan.md) across the five repositories
- [Glossary](CONTEXT.md)
- [Decisions](docs/adr/)
- [Consumer fixture examples](examples/agentctl/)

## Versioning

One version line: the tag `vX.Y.Z` is the contract version and `X` is the contract major. The library and `agentfleet` are released with it. Engines and consumers depend on this repository as a git dependency pinned to a tag and run its fixtures and conformance suite in their own CI.

## Development

Requires [uv](https://docs.astral.sh/uv/). `uv sync` sets up the environment; `uv run pytest` runs the tests. See [AGENTS.md](AGENTS.md#stack) for the stack conventions and the full check.
