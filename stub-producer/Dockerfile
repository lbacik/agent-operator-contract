# Stub producer image: a minimal, test-only engine for the producer suite.
# Built in CI and never published.
FROM python:3.11-slim

RUN useradd --create-home --shell /bin/false agent

COPY src/agent_operator_contract /opt/contract/agent_operator_contract
COPY stub-producer/agentctl /usr/local/bin/agentctl
RUN chmod +x /usr/local/bin/agentctl \
    && PYTHONPATH=/opt/contract python3 -c \
        "import agent_operator_contract.shared; print('shared part loads')"

ENV PYTHONPATH=/opt/contract

USER agent

CMD ["sleep", "infinity"]
