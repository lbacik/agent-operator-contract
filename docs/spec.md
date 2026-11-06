# Operator contract, major version 1

**Status:** draft for `v1.0.0`. The `v1.0.0` tag is cut when the JSON Schemas, conformance suite, shared library and `agentfleet` exist (adoption step 1 in [the adoption plan](./adoption-plan.md)). Until then this prose is the normative source.

**Provenance.** Agreed with the owner in [What versioned operator contract and adoption plan should all four repositories implement?](https://github.com/lbacik/coding-agent/issues/192), building on [What common operator capabilities define an Agent Forge-compatible agent?](https://github.com/lbacik/coding-agent/issues/190#issuecomment-6680989084) (the capability surface) and [Where should the shared agent interface live, and what should it own?](https://github.com/lbacik/coding-agent/issues/191#issuecomment-6681592096) (the repository allocation). Source facts were read at simple-coding-agent `38aaee1`, coding-agent `9a13c34`, agent-forge `04d6eff` and agent-dashboard `b1e5ff4`.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119. Terms in **bold capitals** are defined in [`CONTEXT.md`](../CONTEXT.md).

## Contents

1. [Roles](#1-roles)
2. [Deployment and discovery](#2-deployment-and-discovery)
3. [Invocation](#3-invocation)
4. [Reply envelope, errors and exit codes](#4-reply-envelope-errors-and-exit-codes)
5. [Versions and Operator Capabilities](#5-versions-and-operator-capabilities)
6. [Operations](#6-operations)
7. [Shared types](#7-shared-types)
8. [Operator Command semantics](#8-operator-command-semantics)
9. [Observation semantics](#9-observation-semantics)
10. [Contract Events](#10-contract-events)
11. [Attempt Archives, sealing and export](#11-attempt-archives-sealing-and-export)
12. [Offline Read](#12-offline-read)
13. [Security and redaction](#13-security-and-redaction)
14. [Failure, restart and race scenarios](#14-failure-restart-and-race-scenarios)
15. [Conformance](#15-conformance)
16. [Backend mapping notes (informative)](#16-backend-mapping-notes-informative)

## 1. Roles

- **Producer:** an engine image that serves the contract through its in-container `agentctl`. Today: simple-coding-agent and coding-agent.
- **Consumer:** anything that calls a producer: `agentfleet`, agent-forge's skills (through `agentfleet`), agent-dashboard (through the shared library's client part).
- **Deployment adapter:** per-backend Forge templates that build, configure and start a producer. They are not part of the contract except for the discovery obligations in §2.

The contract normalizes only the in-container CLI: its argv, its stdout and its exit code. Everything behind it (IPC, sockets, databases, file layouts, stdout logging of the main process) is private to each producer.

## 2. Deployment and discovery

### 2.1 Discovery labels

A deployment MUST set these Docker labels on the managed service's container, through the deployment (for example Compose `labels:`), not as an image `LABEL`. Images are shared with one-off services (coding-agent runs `agent`, `validate` and `worker` from one image), and only the managed service is a **Managed Instance**.

| Label | Value | Use |
| --- | --- | --- |
| `io.github.lbacik.agent-operator.contract` | contract major, e.g. `1` | Presence makes the container a **Fleet** member. Consumers choose the protocol from it. It is a hint, not proof: compatibility is decided at runtime (§5). |
| `io.github.lbacik.agent-operator.instance` | instance name, `^[a-z0-9][a-z0-9-]{0,62}$` | Unique on the host. The address in `agentfleet <op> <instance>`. |
| `io.github.lbacik.agent-operator.exec-user` | OS user name | The user for `docker exec` and Offline Read. MUST NOT be `root`. |
| `io.github.lbacik.agent-operator.backend` | backend name, e.g. `coding-agent` | Display and deployment-adapter selection only. |

Consumers MUST ignore Compose one-off containers (`com.docker.compose.oneoff=True`) even when labelled. If two containers on a host carry the same instance name, consumers MUST refuse to address that name and report the duplicate.

Contract images MUST NOT carry the legacy `io.github.lbacik.simple-coding-agent.role` label, so a consumer that has not adopted the contract does not mistake a contract instance for a legacy one.

### 2.2 Image obligations

- The contract CLI MUST be executable at **`/usr/local/bin/agentctl`** by the exec user, also when the root filesystem is read-only.
- It MUST run without a TTY and without network access for Offline Read operations.
- Durable state MUST live on one or more Docker volumes declared by the image or deployment. Offline Read (§12) mounts all of them read-only.
- No restart policy is imposed. coding-agent keeps `restart: "no"`.
- No published port, no HTTP endpoint and no shared secret token are part of the contract.

### 2.3 Readiness

"Container running" is not readiness. A Managed Instance is **ready** when `status` answers with `instance.state` other than `starting`. A producer MUST start serving the contract right after it has parsed its configuration and acquired its volume lock, before its remaining startup checks, and MUST report the check in progress as `instance.startup_check`.

## 3. Invocation

### 3.1 Online

```text
docker exec --user <exec-user> <container> /usr/local/bin/agentctl --contract 1 <operation> [arguments]
```

No `-t`. stdin is not used. `--contract <major>` MUST be the first argument; it states the major the consumer speaks.

### 3.2 Offline

```text
docker run --rm --network none --user <exec-user> \
  --mount type=volume,src=<volume>,dst=<path>,readonly   # once per volume of the container
  --entrypoint /usr/local/bin/agentctl <image-id> --contract 1 --offline <operation> [arguments]
```

The image ID and the volume mounts are taken from `docker inspect` of the stopped container. See §12.

### 3.3 Time bounds

| Operation class | Producer budget | Consumer default timeout |
| --- | --- | --- |
| `identity`, `status`, `command` | answer within 3 s, also while a mutation waits on the engine's lock | 10 s |
| mutations | commit or reject within 10 s | 15 s |
| `attempts list`, `attempts show` | answer within 5 s | 15 s |
| `attempts export` | start streaming within 5 s | no total bound; 30 s without progress |

A consumer timeout is an ambiguous outcome for a mutation (§8.4).

## 4. Reply envelope, errors and exit codes

Every operation except a successful `attempts export` writes **exactly one JSON document** followed by a newline to stdout, also for usage errors. stderr is free-form diagnostic text and MUST NOT be parsed.

```json
{"contract": "1.0", "ok": true, "result": {}}
```

```json
{"contract": "1.0", "ok": false,
 "error": {"code": "repository_mismatch", "message": "expected acme/widgets, this instance targets acme/gadgets",
           "retryable": false, "request_id": "req-3f1c9a2b7d10", "details": {}}}
```

- `contract` is the producer's `major.minor`.
- `error.code` is the machine-readable cause. `message` is for humans and MUST NOT be parsed.
- `retryable: true` means the same request (same request ID, same payload) may be sent again.
- `request_id` is present when the failed call was a mutation that carried one.
- `details` is an object of code-specific fields; consumers MUST ignore unknown keys.

| Exit | `error.code` | Meaning | `retryable` |
| --- | --- | --- | --- |
| 0 | — | Read succeeded, or mutation committed (§8). | — |
| 1 | `rejected` | Valid operation refused in the current state. Nothing stored. | false |
| 1 | `repository_mismatch` | `--expect-repo` differs from the Target Repository. Nothing stored. | false |
| 1 | `payload_mismatch` | Request ID already used with a different payload. | false |
| 1 | `invalid_argument` | Well-formed argv, invalid value (e.g. `--after 0`, bad request ID). | false |
| 2 | `usage` | Malformed argv. | false |
| 3 | `unsupported` | Operation or capability not advertised. No request ID consumed, nothing stored. | false |
| 4 | `contract_mismatch` | Consumer's `--contract` major is not the producer's. `details.supported: [<major>]`. Nothing executed. | false |
| 5 | `not_found` | Unknown request ID, Attempt or archive path. | false |
| 6 | `agent_unreachable` | The CLI could not reach the engine process. For a mutation the outcome is **ambiguous**. | true |
| 7 | `internal` | The engine answered but could not serve the request. For a mutation, ambiguous unless `details.stored: false`. | true |

Exit codes 125, 126 and 127 belong to Docker (daemon error, not executable, not found) and are transport failures for the consumer; for a mutation they are ambiguous. A consumer MUST decide on `error.code`, using the exit code only as a coarse class and as a fallback when stdout is not a valid envelope.

### 4.1 Check order

A producer evaluates a call in this order and stops at the first failure: `--contract` major → argv grammar (`usage`) → capability (`unsupported`) → argument values (`invalid_argument`) → `--expect-repo` (`repository_mismatch`) → idempotency (`payload_mismatch` or replay) → state (`rejected`) → commit.

## 5. Versions and Operator Capabilities

- The contract version is `major.minor`. A minor adds optional capabilities, optional fields or new error `details`; it never removes or retypes a field, never changes a meaning, and never makes an optional field required. A major is breaking.
- A producer serves exactly one major. A consumer MUST send `--contract <major>` on every call; a producer MUST reply `contract_mismatch` (exit 4) without executing anything when it differs.
- Consumers MUST ignore unknown fields and unknown enum values in optional fields (rendering them as "unknown"), and MUST NOT infer compatibility from the backend name, the image tag or the library version.
- `agentfleet` and the client library support majors N and N−1. With major 1 there is no N−1: the legacy `agentctl` protocol of simple-coding-agent is **not** a contract version and `agentfleet` does not speak it.

### 5.1 Operator Capabilities in major 1

The mandatory core is never listed; it is implied by the major. `identity.capabilities` lists the optional capabilities the producer supports.

| Capability | Operation | Notes |
| --- | --- | --- |
| *(core)* | `identity`, `status`, `stop`, `resume`, `command`, `attempts list`, `attempts show`, `attempts export`, Offline Read | Mandatory. |
| `stop-after` | `stop --after N` | |
| `next-issue` | `next issue N` | |
| `operator-handoff` | `handoff now` | |
| `recovery-retry` | `recovery retry <attempt>` | |
| `recovery-release` | `recovery release <attempt> --saved-at <location>` | |
| `errors-reset` | `errors reset` | |
| `pool` | — (declaration only) | Same-repository Pool coordination. Without it, a deployment adapter MUST refuse a second instance for the same Target Repository on the host. |

A consumer SHOULD hide or refuse an operation whose capability is not advertised, before submitting it. A producer MUST still answer `unsupported` (exit 3) if one arrives.

## 6. Operations

Every operation is `agentctl --contract 1 [--offline] <operation>`. Operations not available offline answer `unsupported` with `--offline`.

### 6.1 Reads

| Operation | Offline | Result |
| --- | --- | --- |
| `identity` | yes | [`Identity`](#71-identity) |
| `status` | yes | [`StatusSnapshot`](#72-statussnapshot) |
| `command <request-id>` | yes | `{"command": CommandRecord}`; unknown ID → `not_found` |
| `attempts list [--limit N] [--before <cursor>]` | yes | `{"attempts": [AttemptSummary], "next_cursor": string\|null}`, newest first. `N` defaults to 20, maximum 100. The cursor is opaque. |
| `attempts show <attempt-id>` | yes | `{"attempt": AttemptSummary, "manifest": [ManifestEntry]}` |
| `attempts export <attempt-id> [--path <path>]...` | yes | A tar stream (§11.3). |

### 6.2 Mutations

Every mutation requires:

- `--request-id <id>` matching `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`, generated by the consumer **before** submission;
- `--expect-repo <owner/name>`, compared case-insensitively with the Target Repository.

| Operation | Capability | Effect |
| --- | --- | --- |
| `stop` | core | Close Intake (drain). |
| `stop --after N` | `stop-after` | Close Intake after N more Attempts with a Terminal Outcome, `1 ≤ N ≤ 999`. |
| `resume` | core | Reopen Intake; supersede a pending stop or stop plan. |
| `next issue N` | `next-issue` | One-shot priority for issue N among eligible issues. |
| `handoff now` | `operator-handoff` | Operator Handoff of the active Attempt. |
| `recovery retry <attempt-id>` | `recovery-retry` | Retry safe finalization from retained evidence. |
| `recovery release <attempt-id> --saved-at <location>` | `recovery-release` | Record where work was secured, release the issue, finalize. |
| `errors reset` | `errors-reset` | Reset the durable infrastructure-error guard. |

A successful mutation (exit 0) returns `{"command": CommandRecord, "replayed": bool}`. `replayed: true` means the request ID already existed with an identical payload and nothing new happened.

## 7. Shared types

Timestamps are RFC 3339 UTC strings with a `Z` suffix. All objects MAY gain fields in a minor version.

### 7.1 Identity

```json
{
  "backend": "coding-agent",
  "agent_version": "0.9.0",
  "contract": {"major": 1, "minor": 0},
  "repository": "acme/widgets",
  "mode": "online",
  "process_started_at": "2026-11-06T08:00:00Z",
  "capabilities": ["next-issue", "stop-after"]
}
```

`mode` is `online` or `offline`. `process_started_at` is `null` offline. `capabilities` is sorted.

### 7.2 StatusSnapshot

```text
generated_at          timestamp           when this document was produced
identity              Identity
instance              { state, startup_check: string|null, last_halt: Halt|null, observed_at }
intake                { state: "open"|"closed", pending_stop: CommandRef|null,
                        stop_plan: { request_id, remaining: int, counted: int }|null,
                        priority: { request_id, issue: int }|null, observed_at }
current_attempt       CurrentAttempt|null       null when no Attempt is active
last_finished         AttemptSummary|null
recent_commands       [CommandRecord]           at most 20, newest last
recent_events         [ContractEvent]           at most 50, newest last, process-local
truncated             bool
```

`instance.state` is one of:

| State | Meaning |
| --- | --- |
| `starting` | Startup checks running; `startup_check` names the current one. |
| `recovering` | Finishing recorded effects of earlier Attempts. |
| `idle` | Intake open, no active Attempt. |
| `active` | Intake open, an Attempt is active. |
| `draining` | Intake closed, an Attempt is still active. |
| `paused` | Intake closed, no active Attempt. |
| `stopped` | Offline only: the process is not running. |

`Halt` is `{ "reason": string, "detail": string, "at": timestamp }`, recorded durably by the engine before it exits and reported on the next start and offline.

`CurrentAttempt`:

```text
attempt_id            string            native, opaque
issue                 { number, title }
native_phase          string            engine-native phase name, displayed as-is
started_at            timestamp
last_activity_at      timestamp|null
observed_at           timestamp
budget                Budget
figures               { <name>: Figure }   optional figures, §9.2
```

`Budget`:

```text
raw_tokens            Figure (tokens)   cache-inclusive total
effective_tokens      Figure (tokens)   engine's effective accounting
enforced_tokens       "raw"|"effective"|"none"
limits                [ { name, used: Figure, limit: Figure } ]
```

`limits[].name` is one of `raw_tokens`, `effective_tokens`, `cost_usd`, `wall_clock_seconds`, `tool_calls`, `turns`, or `native.<name>`. Only ceilings the engine enforces are listed.

### 7.3 Figure

```json
{"value": 1834221, "unit": "tokens", "basis": "measured"}
{"value": null, "unit": "ratio", "basis": "not_applicable", "reason": "backend has no prompt cache metric"}
```

- `unit`: `tokens`, `usd`, `seconds`, `count` or `ratio` (0 to 1).
- `basis`: `measured`, `estimated`, `configured`, `assumed`, `unavailable` or `not_applicable`. With `unavailable` and `not_applicable`, `value` MUST be `null`; with any other basis it MUST NOT be `null`.
- `reason`: optional human text.

### 7.4 AttemptSummary

```text
attempt_id            string
issue                 { number, title|null }
started_at            timestamp
finished_at           timestamp|null
lifecycle_state       string            native (e.g. "finished", "released")
outcome               { native: string, reason: string|null, label: string }|null
delivery              { pull_request: { number, url } }|null
attention_required    bool              work retained or a human decision pending
sealed                bool
sealed_at             timestamp|null
```

There is no shared outcome enum. `outcome.label` is a backend-supplied display text. Consumers use `outcome`, `delivery` and `attention_required` for display and alerting only, never to drive behavior.

### 7.5 CommandRecord

```text
request_id            string
sequence              int               per-volume, strictly increasing in commit order
kind                  "stop"|"stop_after"|"resume"|"next_issue"|"handoff"
                      |"recovery_retry"|"recovery_release"|"errors_reset"
arguments             object            e.g. {"after": 3}, {"issue": 42}, {"attempt_id": "...", "saved_at": "..."}
repository            string
acknowledgement       "accepted"|"completed"|"superseded"|"not_fulfilled"
detail                string            human-readable effect or reason, single line
created_at            timestamp
updated_at            timestamp
```

`CommandRef` is `{ "request_id", "sequence" }`.

### 7.6 ManifestEntry

```text
path                  relative POSIX path, no "..", no leading "/"
kind                  "events"|"transcript"|"tool_call"|"tool_result"|"model_output"
                      |"command_output"|"record"|"other"
media_type            e.g. "application/x-ndjson", "application/json", "text/plain"
size                  int, bytes
sha256                lowercase hex
```

## 8. Operator Command semantics

### 8.1 Guarantees

- **Durable request ID.** A committed command is stored on the state volume for the volume's lifetime and is never deleted, so a retried ID can never execute twice.
- **Idempotency.** The canonical payload is `kind` plus `arguments`; `--expect-repo` is not part of it. The same ID with the same payload returns the stored record (`replayed: true`). The same ID with a different payload is `payload_mismatch`.
- **Order.** Commands are committed in `sequence` order.
- **Success means committed.** Exit 0 means the command is durably recorded. Its effect may still be pending (`accepted`).
- **Wrong-instance guard.** `--expect-repo` is required on every mutation, including recovery and error reset.
- **Every command reaches a final acknowledgement** (`completed`, `superseded` or `not_fulfilled`). No restart leaves a command `accepted` forever.

### 8.2 Acknowledgement transitions

```text
commit ──► accepted ──► completed
   │           ├──────► superseded
   │           └──────► not_fulfilled
   └──► completed | not_fulfilled      (effect decided at commit time)
```

Supersession: a newer `stop`, `stop --after` or `resume` supersedes a pending one of the others; a newer `stop --after` supersedes the previous plan; a newer `next issue` supersedes the previous priority.

### 8.3 Per-command semantics

- **`stop`** serializes with the Claim boundary. Committed before it: no Claim opens and the command is `completed` immediately. Committed after it: `accepted`; the active Attempt ends normally and the command becomes `completed` only when that Attempt reaches its engine's final lifecycle state (the archive need not be Sealed yet). Recording an outcome is not enough. Stop never interrupts model work and is never an alias of SIGTERM or `docker stop`.
- **Intake is durable.** It persists across process and container restarts. A fresh volume starts with Intake open. A drain interrupted by a restart still holds after startup. Commands committed during `starting` or `recovering` apply at the first Claim boundary, so a stop issued during startup prevents the first Claim.
- **`resume`** only reopens closed Intake and supersedes a pending stop or stop plan. It never starts a process and never resumes interrupted work. Starting a stopped container is a deployment action, not a command.
- **`stop --after N`** counts fully finalized Attempts that have a Terminal Outcome, the active one first. A Claim released before confirmation does not count. The counter is durable. The command is `completed` when Intake closes.
- **`next issue N`** is a one-shot priority among already eligible issues. Eligibility is checked at commit (`rejected` if the issue is plainly ineligible) and again at the Claim boundary. It never bypasses selection labels, assignees, blockers or delivery guards and never reopens Intake. Ineligible at Claim: `not_fulfilled`, and normal selection proceeds.
- **`handoff now`** is `completed` only when publication, issue release and cleanup have finished durably; otherwise `not_fulfilled` and the work is retained.
- **`recovery retry`** retries safe finalization from retained evidence only. It never reruns the model or recomputes a recorded verdict.
- **`recovery release`** records `--saved-at`, releases the issue and finalizes. No automatic requeue.
- **`errors reset`** resets the durable infrastructure-error guard of a live process.

### 8.4 Ambiguous outcomes

`agent_unreachable`, `internal` (unless `details.stored: false`), a consumer timeout and Docker exit codes 125–127 leave a mutation's outcome unknown. The consumer MUST retry with the **same request ID and payload**; `agentfleet` does so within one invocation, then prints the request ID so the operator can use `command <request-id>`. A consumer MUST NOT generate a new request ID for an ambiguous retry.

## 9. Observation semantics

### 9.1 Rules

- **Unknown is never zero.** A value the engine does not know is `unavailable`; a concept the backend lacks is `not_applicable`. Internal zero defaults MUST NOT surface as `measured` zeros.
- **Required figures** (`budget.raw_tokens`, `budget.effective_tokens`, every listed `limits[]` entry) are always present with an explicit basis. **Optional figures** may be absent; absence means "not reported by this backend", never zero.
- **Clocks.** `generated_at` (document produced), `identity.process_started_at` (process start) and each section's `observed_at` (when that section's data was last read) are distinct. A consumer judges freshness per section. There is no `sequence` counter: it measured serving, not progress.
- **Bounds.** A Status Snapshot is at most 256 KiB. Over the bound the producer drops the oldest `recent_events` first, then optional figures, then the oldest `recent_commands`, and sets `truncated: true`.
- **Responsiveness.** `identity`, `status` and `command` MUST answer within their budget while a mutation waits on the engine's lock, and MUST NOT perform network, git or provider I/O. A producer serves prepared state.

### 9.2 Optional figures

| Name | Unit | Notes |
| --- | --- | --- |
| `cost_usd` | usd | Basis `estimated` when computed from configured prices. |
| `cache_hit_rate` | ratio | Main conversation only. |
| `context_used`, `context_peak`, `context_window` | tokens | |
| `turns` | count | Model turns; tool calls are not turns. |
| `tool_calls` | count | |
| `continuations` | count | Engine-native context continuations. |
| `review_seats_running` | count | Engine-native review participants; not generic sub-agents. |
| `elapsed_seconds` | seconds | |
| `native.<name>` | any | Backend-specific; consumers may ignore. |

## 10. Contract Events

A Contract Event is `{ "at", "level", "event", "attempt_id"?, "phase"?, "text" }` with `level` in `debug|info|warning|error` and `text` at most 300 characters, redacted.

| Event | When | Extra fields |
| --- | --- | --- |
| `process_started` | Process start, before startup checks | `agent_version` |
| `startup_check` | Each startup check begins or ends | `check`, `result: started\|ok\|skipped\|failed` |
| `ready` | First state other than `starting` | |
| `halted` | Engine decides to exit (recorded before exit) | `reason` |
| `claim_opened` | A Claim opens | `attempt_id`, `issue` |
| `phase_changed` | Native phase changes | `attempt_id`, `phase` |
| `outcome_recorded` | Engine records the Attempt's outcome | `attempt_id`, `outcome`, `reason` |
| `attempt_finished` | Attempt reaches its final lifecycle state with an outcome | `attempt_id` |
| `attempt_released` | Attempt ends without a Terminal Outcome | `attempt_id` |
| `sealed` | Attempt Archive becomes Sealed | `attempt_id` |
| `command_accepted` | A command is committed | `request_id`, `kind`, `acknowledgement` |
| `command_resolved` | A command reaches a final acknowledgement | `request_id`, `acknowledgement` |

Native events MUST use the `native.` prefix; consumers MAY ignore them. `status.recent_events` holds the newest 50 events of the current process. Each Attempt Archive holds that Attempt's events in `events.jsonl`. The main process's container stdout is not part of the contract.

## 11. Attempt Archives, sealing and export

### 11.1 Contents

- `events.jsonl` (kind `events`) is required in every Attempt Archive.
- Every other artifact is backend-defined and described by its `kind` and `media_type`. Consumers render by `kind` and `media_type`, never by backend-specific file names.
- The manifest MUST NOT include private engine state: databases, credentials, checkpoints, configuration containing secrets, raw unredacted transcripts.
- An Attempt is listed as soon as its Claim opens.

### 11.2 Sealed

An archive is **Sealed** when its evidence set is complete and immutable: the Attempt reached its final lifecycle state and every artifact and metadata backfill the engine will ever write for it is written. After sealing, the manifest and every file are immutable; `sealed_at` is set once.

- Consumers MUST cache only Sealed archives, keyed by (instance, backend, repository, attempt ID) and verified by `sha256`.
- An unsealed archive (`sealed: false`) MAY be read, including the active Attempt's, but is a moving snapshot.
- Recording an outcome is not sealing. A consumer MUST NOT treat the presence of an outcome as finality.

### 11.3 Export

`attempts export` is the only operation whose successful stdout is not a JSON envelope.

- The stream is an uncompressed POSIX tar (pax). Its first member is `manifest.json`: `{"contract": "1.0", "attempt": AttemptSummary, "files": [ManifestEntry]}`. The files follow under their manifest paths.
- With `--path`, only the named files are exported, and `files` lists only those. An unknown path is `not_found` before streaming starts.
- An error detected before streaming starts produces the JSON envelope and a non-zero exit. A non-zero exit after streaming started means the consumer MUST discard the output.
- A file whose content does not match its manifest `sha256` (possible only for an unsealed archive) MUST be treated by the consumer as a torn read and fetched again.

## 12. Offline Read

A stopped Managed Instance remains observable without starting its engine, which would resume work.

- The consumer MUST check `docker inspect` that the container is not running, then runs the image with every volume of the container mounted read-only and with no network (§3.2).
- The producer MUST serve `identity`, `status` (with `instance.state: "stopped"`, `last_halt`, `last_finished`, `recent_commands`), `command`, `attempts list`, `attempts show` and `attempts export` from the volume alone, without taking the volume lock and without writing anything.
- If the producer detects a live engine holding the volume, it MUST answer `rejected` with `details.reason: "instance_running"`.
- Mutations answer `unsupported` offline.
- `recent_events` offline MAY be empty; the per-Attempt `events.jsonl` remains available.

## 13. Security and redaction

- Producers own redaction. No reply, Contract Event, manifest or exported file may contain credentials, tokens, private keys or secret configuration values.
- The exec user MUST NOT be root. The contract requires no published port and no network listener; private IPC MUST be accessible only to the exec user (for example a 0700 directory with a 0600 socket).
- Offline Read runs with `--network none` and read-only mounts.
- `--saved-at` is stored and echoed; operators pass a location, never a credential.
- Consumers MUST NOT pass secrets through argv.

## 14. Failure, restart and race scenarios

Each row is a conformance scenario (§15).

| Scenario | Required behavior |
| --- | --- |
| Mutation commits, the process dies before replying | Consumer sees `agent_unreachable` or a transport error, retries with the same ID after restart, gets the stored record with `replayed: true`; one effect only. |
| Same request ID, different payload | `payload_mismatch`, nothing stored. |
| `stop` before the Claim boundary, then a crash | After restart Intake is still closed, no Claim opens, the command is `completed`. |
| `stop` after the Claim boundary, then SIGTERM | The Attempt is interrupted; recovery at the next start finishes recorded effects; the Attempt reaches its final lifecycle state; the stop becomes `completed`. |
| `stop` during `starting` | The first Claim never opens. |
| `resume` while a stop is `accepted` | The stop becomes `superseded`; Intake stays open. |
| `next issue N` committed, restart before Claim | The priority still applies; eligibility is re-checked at Claim; ineligible → `not_fulfilled`. |
| `stop --after N` in progress, restart | The counter survives and continues. |
| `handoff now` in progress, crash | `not_fulfilled`, partial work retained. |
| Unadvertised capability invoked | `unsupported`, exit 3, the request ID is not consumed and may be reused for a supported operation. |
| `--contract 2` against a major-1 producer | `contract_mismatch`, exit 4, nothing executed. |
| Wrong `--expect-repo` on any mutation | `repository_mismatch`, nothing stored. |
| Status during a long-held engine lock | Answers within 3 s with the last prepared state and older `observed_at`. |
| Outcome recorded, finalization still writing | `sealed: false`; consumers do not cache; `sealed` event follows. |
| Export of an unsealed archive while a file grows | The torn read is detected by `sha256` mismatch. |
| Halt (process exit by engine decision) | `halted` event recorded; Offline Read and the next start report `last_halt`. |
| Offline Read while the container is running | Consumer refuses; producer answers `rejected` / `instance_running` if reached. |
| Publication effect pending at restart | Recovery finishes it before new Claims; `instance.state` is `recovering` meanwhile; no command changes a recorded verdict. |

## 15. Conformance

A producer or consumer is compatible when it passes the conformance suite of the tag it pins, whether or not it uses the shared library.

- **Producer suite.** Black-box scenarios run against a producer container. Each engine supplies a **conformance harness** that drives its own fakes (provider, GitHub) through a small control API: make an issue eligible, hold an Attempt at a phase, finish an Attempt with a given outcome, crash the process, stop the container. The suite covers every row of §14, every operation in §6 including `unsupported` for each unadvertised capability, Offline Read, and validates every reply against the JSON Schemas. The exact harness API is defined in adoption step 1.
- **Consumer suite.** Recorded fixtures `{argv, exit_code, stdout, stderr}` (see [`examples/agentctl/`](../examples/agentctl/)) and a fake `agentctl` that replays them. A consumer passes when it classifies every fixture correctly: success, rejection, `unsupported`, ambiguous outcome, contract mismatch, malformed stdout.
- Engines and consumers run both from the same pinned tag in their own CI, without copying fixtures.

## 16. Backend mapping notes (informative)

These notes record how each engine's native concepts map onto the contract. Each engine's adoption decides the details.

### simple-coding-agent

- Intake `running` → `open`; `stopping` → `closed` with an active Attempt (`draining`); `stopped` → `closed` (`paused`).
- `attempt_id`: the checkpoint `started_at` value. Native phases: `claimed`, `setup`, `model_running`, `pushing`, `publishing`.
- Native outcomes: `complete`, `incomplete`, `infrastructure_error`, `no_changes`, `handoff`.
- Sealing happens only after the whole finalization commit, including the `pull_request` backfill into `attempt.json`, not when `outcome` is first written.
- Token budget is cache-inclusive: `enforced_tokens: "raw"`. Telemetry v1's `sdk_assumed` basis becomes `assumed`.
- Advertises `stop-after`, `next-issue`, `operator-handoff`, `recovery-retry`, `recovery-release`, `errors-reset` and, where configured, `pool`.
- Without `--contract`, `agentctl` keeps the legacy grammar until adoption step 5.

### coding-agent

- `attempt_id`: `#<issue>/<n>`. Lifecycle states: `open`, `claim-confirmed`, `outcome-recorded`, `finished`, `released`. Native phases: the Attempt graph nodes (`claim` … `terminal_writes`).
- Terminal Outcomes: `delivered`, `failed`, `failed-limit`, `abandoned`, each with its native reason. The Run Ledger stays the sole lifecycle authority; contract DTOs are projections of it.
- Budget: `enforced_tokens: "effective"` (cache reads excluded); limits from the configured ceilings (`effective_tokens`, `cost_usd`, `wall_clock_seconds`, `tool_calls`). There is no turn limit.
- `cost_usd` has basis `estimated` (configured prices). SDK-specific figures are `not_applicable`.
- Advertises `stop-after` and `next-issue` only. `handoff now`, recovery commands, `errors reset` and `pool` answer `unsupported`.
- There is no legacy grammar: without `--contract`, `agentctl` answers `usage`.
