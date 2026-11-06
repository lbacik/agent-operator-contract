---
status: accepted
---

# Stopped instances are read offline from their own volume, through the contract

The read-only part of the contract is mandatory in an offline mode: `identity`, `status`, `command` and `attempts list|show|export`. A consumer runs the stopped container's image with `agentctl --contract 1 --offline`, every state volume mounted read-only and no network. The engine serves from its volume without taking the lock. Decided in [What versioned operator contract and adoption plan should all four repositories implement?](https://github.com/lbacik/coding-agent/issues/192).

## Why

coding-agent runs with `restart: "no"` and exits on a Halt. The most important Attempts, the ones that explain the Halt, therefore belong to a stopped container, where `docker exec` cannot run. Starting the instance to read them would resume work. The existing workaround, `docker cp` of `/data/logs` (agent-dashboard ADR 0005), is exactly the file-layout dependency the contract removes.

## Considered options

- **Contract for running instances only.** Archives of stopped instances would be unreachable without resuming work.
- **A published archive layout readable with `docker cp`.** It would freeze each engine's storage and could not honour sealing or redaction decisions made in code.

## Consequences

Each engine must read its own state from a read-only mount. For coding-agent this means opening a WAL-mode SQLite Run Ledger read-only, which is the first risk to prove in its adoption.
