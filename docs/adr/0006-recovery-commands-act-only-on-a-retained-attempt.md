---
status: accepted
---

# Recovery commands act only on a Retained Attempt

`recovery retry` and `recovery release` act only on a Retained Attempt: one whose model work has ended but whose finalization was interrupted or not fulfilled. On an active Attempt both answer `rejected` and store nothing. On an Attempt that is already finalized, `recovery release` answers `rejected`, while `recovery retry` is a no-op `completed`. An unknown Attempt ID is `not_found`. While a Retained Attempt exists it is the `current_attempt`, `instance.state` is `recovering`, and no Claim opens. Decided in [lbacik/agent-operator-contract#36](https://github.com/lbacik/agent-operator-contract/issues/36) and released as a clarification in `v1.0.2`.

## Why

Spec §8.3 of `v1.0.0` said only that `recovery release` "records `--saved-at`, releases the issue and finalizes". The v1.0.1 producer suite read that as permission to release a running Attempt and to flip a finished one to `released`, and the stub producer followed it. An engine with real retained work cannot do either safely. Releasing a running Attempt aborts model work on the operator's word that the work is saved, which `stop` deliberately never does. A finalized Attempt has already released its issue and has nothing left to secure, so recording `--saved-at` for it would be false.

## Considered options

- **Release any known Attempt (the v1.0.1 suite).** This turns recovery into a way to interrupt model work, which is what `handoff now` is for, and lets an operator record a location for work that does not exist.
- **No-op `completed` for releasing a finalized Attempt.** This would match `recovery retry`. It was rejected because release carries new information (`--saved-at`), and a success would claim it was applied. Retry is an idempotent "finish finalization", which is already true for a finalized Attempt.
- **Show a Retained Attempt in `last_finished` with `attention_required: true`.** This was rejected because `last_finished` means finalized, and a Retained Attempt is not.

## Consequences

- The Conformance Harness gains the one-shot `fail-finalization` verb (harness protocol `1.1`) so the suite can produce a Retained Attempt on demand.
- An engine that cannot fail its own finalization answers `unsupported`, and the recovery scenarios do not run.
- Whether `resume` is rejected while a Retained Attempt exists is left to the engine.
