---
status: accepted
---

# `agentctl` is the producer CLI in every engine; `--contract` and a deployment label select the protocol

Every contract image serves the contract from `/usr/local/bin/agentctl`, including coding-agent, which had no `agentctl`. simple-coding-agent's existing `agentctl` changes in place: with `--contract <major>` it speaks the contract, and without it the legacy grammar remains until the legacy paths are retired. Consumers choose the protocol from the `io.github.lbacik.agent-operator.contract` label set by the deployment. The label is not set as an image `LABEL`, because images are shared with one-off services. The runtime `--contract` check, not the label, decides compatibility. Decided in [What versioned operator contract and adoption plan should all four repositories implement?](https://github.com/lbacik/coding-agent/issues/192).

## Considered options

- **A separate contract executable next to the legacy `agentctl`.** It gives clean separation, at the cost of two CLIs per image, two names in the documentation and a longer coexistence. It was rejected because the label and `--contract` already separate the protocols.
- **Rewriting `agentctl` without a legacy mode.** It would break existing Forge skills and dashboards against any rebuilt image.

## Consequences

- For a transition period, "agentctl" names two protocols. The documentation says "legacy agentctl" and "contract agentctl".
- The host client is `agentfleet`, never `agentctl`, so producer and client are not confused.
- Contract images drop the legacy `io.github.lbacik.simple-coding-agent.role` label, so consumers that have not adopted the contract do not discover them.
