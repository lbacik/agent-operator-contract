# Negative examples

Bare documents (not fixtures) that must be rejected: by the JSON Schema in `schemas/` whose name they start with (`envelope-`, `identity-`, `fixture-`, `harness-request-`, `harness-reply-`, `client-envelope-`, `ls-`, `status-`, `figure-`, `attempt-summary-`, `attempts-list-`, `attempts-show-`, `manifest-entry-`, `export-manifest-`, `contract-event-`, `command-`), and by the matching validator in `agent_operator_contract.documents`. Tests require both sides to agree on every file here, except the library-only examples below.

## Library-only examples

Two examples break a rule across the entries of one manifest (spec section 7.6, since 1.2), which JSON Schema cannot express. They are valid against `schemas/attempts-show.schema.json` and rejected only by `documents.validate_attempts_show`; `documents.LIBRARY_ONLY_NEGATIVES` names them, and tests require the schema to accept them and the library to reject them:

- `attempts-show-duplicate-call-id`: two `tool_result` entries carry the same `call_id`.
- `attempts-show-sealed-result-without-call`: a Sealed archive holds a `tool_result` whose `call_id` no `tool_call` carries.

A consumer that validates with the JSON Schemas alone must check these two rules itself.
