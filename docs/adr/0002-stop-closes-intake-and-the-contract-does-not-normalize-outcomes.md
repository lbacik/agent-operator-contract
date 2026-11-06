---
status: accepted
---

# Stop closes Intake and never aliases SIGTERM; the contract does not normalize outcomes

`stop` closes the durable Intake. The active Attempt finishes normally and the process stays up. `stop` is never an alias of SIGTERM or `docker stop`, which remain interruptions with each engine's own recovery. Outcomes stay native: every Attempt reports its native outcome and reason, a backend-supplied display label, `delivery` and `attention_required`. There is no shared outcome category. Decided in [What common operator capabilities define an Agent Forge-compatible agent?](https://github.com/lbacik/coding-agent/issues/190#issuecomment-6680989084).

## Why

- **Stop.** coding-agent treats SIGTERM as interruption and recovers recorded effects on the next start. Mapping an operator "stop" onto it would turn a drain into an interruption and leave Attempts to recovery. An Intake that survives restarts gives both engines the same drain semantics without changing either engine's shutdown path.
- **Outcomes.** A shared enum would create false equivalences, such as `incomplete` with `failed-limit`, and invite consumers to drive behavior from them. Consumers use outcomes only for display and alerting.
