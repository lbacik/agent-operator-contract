# Negative examples

Bare documents (not fixtures) that must be rejected: by the JSON Schema in `schemas/` whose name they start with (`envelope-`, `identity-`, `fixture-`, `harness-request-`, `harness-reply-`, `client-envelope-`, `ls-`, `status-`, `figure-`, `attempt-summary-`, `attempts-list-`, `attempts-show-`, `manifest-entry-`, `export-manifest-`, `contract-event-`, `command-`), and by the matching validator in `agent_operator_contract.documents`. Tests require both sides to agree on every file here.
