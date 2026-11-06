## Stack

Python, managed with **uv** (`pyproject.toml`, `uv.lock`). Run every Python command through `uv run`; add packages with `uv add`.

- **Runtime is stdlib only.** The shared library and `agentfleet` ship with `dependencies = []` so engines can pin this repository as a git dependency at no cost. Tools needed only to develop or test go in the `dev` dependency group (`uv add --dev`).
- **Python 3.11 is the floor.** `.python-version` pins it so local runs catch newer syntax; CI also runs 3.12 to 3.14.
- **Layout:** the importable package is `src/agent_operator_contract/`; `agentfleet` is its console script. JSON Schemas, fixtures and the error table ship as package data inside the package; the conformance suite lives in this repository beside it.
- **Done means green:** `uv run ruff format --check && uv run ruff check && uv run mypy && uv run pytest`, the same gate as `.github/workflows/ci.yml`.

## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues for lbacik/agent-operator-contract, using the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Uses the five default triage labels: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `CONTEXT.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.
