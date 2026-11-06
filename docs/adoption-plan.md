# Adoption plan for contract major 1

How agent-operator-contract, simple-coding-agent, coding-agent, agent-forge and agent-dashboard adopt [the operator contract](./spec.md). Agreed in [What versioned operator contract and adoption plan should all four repositories implement?](https://github.com/lbacik/coding-agent/issues/192).

The plan covers new instances and fresh state volumes only. Migrating existing volumes, command history or attempt archives is out of scope. Existing simple-coding-agent instances keep running on their current images and are managed through the legacy path until step 5.

## Order

```text
1  agent-operator-contract v1.0.0
        │
2  agent-dashboard + agent-forge: label-gated contract path, legacy path kept
        │
        ├──► 3  simple-coding-agent: contract agentctl (reference producer, first to pass)
        │
        └──► 4  coding-agent: contract agentctl
                     │
5  retire legacy paths (after 3 and 4, when no unlabelled instance remains)
```

Steps 3 and 4 may run in parallel once step 2 is done. simple-coding-agent is preferred first, because a mature producer validates the spec and fixtures. The reason step 2 must come before 3 is weaker than it first looked: simple-coding-agent keeps its legacy grammar when `--contract` is absent (spec §16), so an old Forge skill pointed at a rebuilt image still works. The order mainly guarantees that consumers can manage every new instance from the day it ships.

## Step 1: agent-operator-contract `v1.0.0`

Work:

- JSON Schemas (draft 2020-12) for the envelope, every result type (§7) and the export `manifest.json`, consistent with the prose.
- Fixtures: consumer fixtures under `src/agent_operator_contract/fixtures/agentctl/` covering every exit code and `error.code`, plus Status Snapshot examples for each `instance.state`. The fixtures, the fixture classification index, the negative examples and the JSON Schemas ship as package data.
- Conformance suite:
  - producer scenarios for every row of spec §14, every operation and every unadvertised capability, Offline Read;
  - the conformance harness API that engines implement;
  - the consumer suite with a replaying fake `agentctl`.
- Shared library (stdlib only, Python ≥ 3.11):
  - shared part: DTOs, schema validation, CLI grammar, envelope rendering, exit codes, capability checks;
  - client part: discovery by label, `docker exec` and Offline Read argv, timeouts, same-ID retry of ambiguous mutations, `--contract` and capability checks before submission.
- `agentfleet`: stateless host CLI over the client part. Operations `ls` (Fleet), `identity`, `status`, `stop`, `resume`, `next-issue`, `stop-after`, `handoff`, `recovery-retry`, `recovery-release`, `errors-reset`, `command`, `attempts ls|show|export`. Human output by default, `--json` passthrough.
- CI that runs the schemas against the fixtures and the consumer suite against `agentfleet`.

Exit criteria: tag `v1.0.0`; the producer suite runs against a stub producer in this repository.

## Step 2: consumers learn the contract path

### agent-dashboard

- Discovery:
  - containers with `io.github.lbacik.agent-operator.contract` use the contract path through the client library;
  - containers with only the legacy `io.github.lbacik.simple-coding-agent.role` label keep the telemetry v1 path.
- Contract path:
  - render the Status Snapshot: figures by unit and basis, per-section freshness, `not_applicable` versus `unavailable`;
  - control through the library, with consumer-generated request IDs and same-ID retry;
  - hide actions whose capability is not advertised.
- Replace the [ADR 0003](https://github.com/lbacik/agent-dashboard/blob/2902402d1ee0faf1e74432bdbcda6b8940d586ba/docs/adr/0003-read-attempt-logs-outside-the-agentctl-contract.md) log reading (the in-container `python -c` collector, `docker cp` of `/data/logs`, and the ADR 0005 stopped-instance copy) with:
  - `attempts list|show|export` for contract instances;
  - Offline Read for stopped ones;
  - render artifacts by `kind`/`media_type`;
  - cache Sealed archives only.
- Record a new ADR that supersedes ADR 0003 and ADR 0005 for contract instances.
- Run the consumer conformance suite in CI from the pinned tag.

### agent-forge

- Rewrite the operator skills (status, stop, resume, priority, handoff, recovery, logs, fetch-run, locate-agent, pool-status) to call `agentfleet` and interpret its JSON.
  - Keep the legacy scripts only for instances without the contract label.
  - The `"outcome"` cache heuristic is removed on the contract path; caching follows `sealed`.
- `create-agent` gains per-backend deployment adapters:
  - **simple-coding-agent:** the existing templates, plus the four discovery labels on the managed service, and without the legacy role label in contract images.
  - **coding-agent:**
    - a Compose template with the `worker` service labelled (never `agent` or `validate`);
    - a Workflow Configuration directory mount, the provider and GitHub credential wiring of the coding-agent deployment, and `restart: "no"`;
    - no external Project Profile: the profile stays repository-owned and read at the Base Revision;
    - validation runs coding-agent's own `config validate` and `startup-check`, not simple-coding-agent's profile loader.
- Before a second instance for the same Target Repository, check the Fleet through `agentfleet identity`. Refuse unless the backend advertises `pool`.
- No Python package of its own.

Exit criteria: both consumers pass the consumer suite; legacy instances are still managed as before.

## Step 3: simple-coding-agent as the reference producer

- `agentctl --contract 1` speaks the contract. Without `--contract` it keeps the legacy grammar, marked deprecated.
- Map the existing control store, telemetry and archive onto the contract:
  - one JSON envelope everywhere;
  - distinct exit codes, so an answered `ok:false` is no longer reported as "cannot connect";
  - `--expect-repo` required on every mutation, including recovery and `errors reset`;
  - `not fulfilled` → `not_fulfilled`;
  - `sdk_assumed` → `assumed`;
  - no `sequence` in the snapshot.
- Add the `starting` and `recovering` instance states and the startup-check reporting; serve the contract right after configuration parsing and volume lock.
- Emit Contract Events; write `events.jsonl` per Attempt.
- Seal archives after the full finalization commit, including the `pull_request` backfill; emit `sealed`; expose `sealed`/`sealed_at`.
- Build the manifest from the existing archive files: `agent_output.json` and the tool and model files become `tool_call`, `tool_result`, `model_output` and `record`; check logs become `command_output`. Decide whether redacted transcripts are exported.
- Offline Read from `/data` mounted read-only.
- Remove the image `LABEL io.github.lbacik.simple-coding-agent.role`. Forge templates add the contract labels.
- Implement the conformance harness; pass the producer suite.
- Record an ADR superseding [ADR 0006](https://github.com/lbacik/simple-coding-agent/blob/38aaee14d4dd40d7b58c64ba6ffa5b5b6583dd66/docs/adr/0006-telemetry-through-agentctl-not-http.md)'s one-connection-at-a-time rationale with the responsiveness requirement. Move the contract terms (telemetry snapshot, figure basis, managed instance) to this repository's glossary and link to it.
- Fix the known issues the contract makes visible:
  - the `recovery release` retry hint that omits `--saved-at`;
  - documentation that still calls telemetry serving future work and describes `sequence` per update;
  - example phases and attempt IDs that do not match the code.

## Step 4: coding-agent as the second producer

All existing execution guarantees stay unchanged:

- one Worker per Target Repository and state volume;
- the Run Ledger as the sole lifecycle authority;
- recorded Terminal Outcomes never recomputed;
- the Publication Gate and Delivery Intent;
- the enforced effective-token budget;
- SIGTERM as interruption;
- the repository-owned Project Profile;
- native Attempt IDs and outcomes.

Contract operations are adapters onto existing boundaries.

Work packages, each a candidate issue in coding-agent:

1. **Contract CLI and private IPC.**
   - Install `/usr/local/bin/agentctl` into the image; the root filesystem is read-only, so it is built in.
   - A responsive in-process control listener. Its socket directory must live on a writable, exec-user-only path such as a tmpfs, because `/run` is not writable today.
   - Identity, `status` and `command` are served from prepared state, never through the Worker's thread-bound work and never with network, git or provider I/O.
2. **Durable Intake and the command store.**
   - Intake state and Operator Commands are durable on the state volume.
   - Command acceptance serializes with the Claim boundary and with finalization.
   - Where the command store lives relative to the Run Ledger is decided in coding-agent's own design. The constraint is that the Run Ledger stays the sole lifecycle authority and commands only gate Claims.
   - `stop`, `resume` and command lookup are in scope here.
3. **`stop --after N` and `next issue N`**, on top of package 2 and the existing selection guards.
4. **Startup and readiness.**
   - Serve the contract after configuration parsing and acquisition of `.worker.lock`.
   - Report `starting(check)` and `recovering`.
   - Record a Halt durably before exiting on credential failure, unsupported environment or the third consecutive infrastructure failure.
5. **Truthful usage.**
   - Track availability so that absent usage metadata becomes `unavailable`, not zero.
   - Report raw and effective tokens, configured limits and estimated cost.
6. **Contract Events and archives.**
   - Structured event production replacing prose-only output: today's stdout and stderr are plain text.
   - `events.jsonl` per Attempt.
   - A manifest over the redacted transcript and evidence.
   - Sealing when the Attempt reaches `finished` or `released` and every write is recorded.
   - `attempts list|show|export`.
7. **Offline Read.**
   - Read the Run Ledger and evidence from a read-only mounted volume without the lock.
   - **Risk:** a SQLite database in WAL mode needs special handling on a read-only mount (for example immutable or read-only URI modes, after a clean checkpoint at shutdown). Prove this early.
8. **Conformance harness** over the existing fake provider and fake GitHub; pass the producer suite.
9. **Glossary.**
   - Link the contract's `CONTEXT.md` from coding-agent's.
   - Keep Context-Window Handoff distinct from Operator Handoff.

Advertised capabilities: `stop-after`, `next-issue`.

## Step 5: retire the legacy paths

When no instance without the contract label remains:

- remove the legacy grammar from simple-coding-agent's `agentctl`;
- remove the telemetry v1 path and the legacy log reading from agent-dashboard;
- remove the legacy scripts from agent-forge.

From the next contract major on, the N/N−1 rule applies.

## Risks

| Risk | Where | Mitigation |
| --- | --- | --- |
| Offline Read of a WAL SQLite ledger on a read-only mount | coding-agent | Prove it first in package 7. Fall back to a clean checkpoint on every exit path. |
| Responsive control while Attempts run synchronously | coding-agent | Serve prepared snapshots; never call the Worker's connection from the listener. |
| Archive sealing hides late backfills | simple-coding-agent | Seal only after the full finalization commit. The `sealed` event is part of the conformance suite. |
| Consumers treating labels as proof | all consumers | The runtime `--contract` check and the consumer suite's mismatch fixtures. |
| Two engines diverge on sealing or stop timing | both engines | The same producer scenarios from the same tag. |
