# Agent operator contract

The language shared by coding-agent engines that can be managed interchangeably and by the tools that manage them. Engine-internal terms stay in each engine's own glossary; this file defines only what crosses the contract.

## Instances

**Managed Instance**:
One running or stopped engine container that carries the contract's discovery metadata and manages exactly one Target Repository.
_Avoid_: agent (ambiguous with the engine and the model), worker, container

**Fleet**:
The set of Managed Instances discoverable on one Docker host through the contract's discovery metadata, regardless of backend. A Fleet is not a pool: it implies no shared queue or claim coordination; each member is a single instance under the core contract.
_Avoid_: pool

**Pool**:
Several Managed Instances that coordinate Claims on the same Target Repository. An optional Operator Capability, never implied by a Fleet.
_Avoid_: fleet, cluster

**Backend**:
The engine implementation behind a Managed Instance, such as simple-coding-agent or coding-agent. It selects a deployment adapter and is displayed; it never decides compatibility.
_Avoid_: agent type, flavour

**Target Repository**:
The single repository whose issues a Managed Instance works on, written `owner/name`.
_Avoid_: project, repo (in prose)

## Control

**Operator Capability**:
An advertised operation or feature of the operator contract beyond the mandatory core. Distinct from an engine's Provider Capability Assertion.
_Avoid_: feature flag, permission

**Intake**:
A Managed Instance's durable permission to open new Claims. Closing it drains: the active Attempt finishes and the process stays up.
_Avoid_: queue, pause (in prose), shutdown

**Claim boundary**:
The point at which an engine decides to open a Claim on an issue. Operator Commands serialize with it.
_Avoid_: pickup

**Operator Command**:
A mutation submitted through the contract, identified by a durable request ID and carried through an acknowledgement lifecycle to a final acknowledgement.
_Avoid_: request, action, control message

**Acknowledgement**:
The recorded state of an Operator Command: `accepted`, `completed`, `superseded` or `not_fulfilled`. Success of a submission means the command is committed, not that its effect is complete.
_Avoid_: status, result

**Halt**:
A process exit that an engine chooses because it cannot safely continue (for example a credential failure). It is not a contract state; its reason is reported after the next start.
_Avoid_: crash, stop

**Operator Handoff**:
A cooperative, bounded handover of an active Attempt's partial work, requested by an operator.
_Avoid_: Context-Window Handoff (an engine-internal concept), handoff (unqualified)

## Observation

**Status Snapshot**:
The single bounded JSON document a Managed Instance serves to describe its identity, Intake, current and last finished Attempt, recent commands and recent events.
_Avoid_: telemetry snapshot, telemetry

**Figure**:
A reported quantity with a unit and a Figure Basis. An unknown Figure is never zero.
_Avoid_: metric, stat

**Figure Basis**:
How a Figure was obtained: `measured`, `estimated`, `configured`, `assumed`, `unavailable` (the concept exists but the value is unknown) or `not_applicable` (the concept does not exist in this backend).
_Avoid_: source, confidence

**Contract Event**:
A structured lifecycle event whose name and fields the contract defines. Engines may add native events under the `native.` prefix.
_Avoid_: log line

## Attempts and archives

**Attempt**:
One engine-native try at resolving one issue, identified by its native Attempt ID. Consumers treat the ID as opaque and qualify it with instance, backend and repository.
_Avoid_: run, job

**Attempt Archive**:
The set of redacted artifacts a Managed Instance exposes for one Attempt, described by a manifest.
_Avoid_: logs, run directory

**Sealed**:
An Attempt Archive whose evidence set is complete and immutable. Only a Sealed archive may be cached. Distinct from an engine having recorded the Attempt's outcome.
_Avoid_: final, complete, finished

**Offline Read**:
Serving the read-only part of the contract from a stopped Managed Instance's state volume, mounted read-only into a throwaway container of the same image.
_Avoid_: cold read, docker cp

## Conformance

**Conformance Harness**:
The engine-supplied executable the producer suite drives to steer a Managed Instance through failure and race scenarios: fresh setup and teardown, issue eligibility, Attempt holds and native outcomes, lock holds, crashes, stops, Halts, archive growth and deferred publication. It runs on the host, outside the production image, and answers each verb with one JSON document. A verb or hold point its engine cannot support is answered `unsupported`, and the dependent scenarios are reported as not run.
_Avoid_: fake producer, stub engine, test double
