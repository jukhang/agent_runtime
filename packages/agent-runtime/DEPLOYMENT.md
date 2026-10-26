# Installation and Deployment

Python 3.12 or later is required. The repository keeps its self-contained SDK at
`packages/agent-runtime`; the top-level application from the original development
workspace is not part of this repository.

## Install a release wheel

Download the wheel and SHA256SUMS from the GitHub Release. Verify the checksum before
installing. The project is not published to PyPI by this release; do not install an
unverified same-named package from a public index.

```bash
uv venv .venv
uv pip install --python .venv/bin/python ./agent_runtime-0.1.1-py3-none-any.whl
# Explicit optional capabilities:
uv pip install --python .venv/bin/python './agent_runtime-0.1.1-py3-none-any.whl[storage,provider,mcp]'
```

Core imports do not start services or connect to a database. `provider` installs the
HTTP adapter; `storage` installs SQLAlchemy/Alembic/asyncpg; `mcp` adds schema
validation. Broker and sandbox extras are independent. OpenSandbox requires a
separately configured execution service, pinned image, hard limits and supervision.

## Run the backend examples

```bash
uv sync --project packages/agent-runtime/examples/python --locked
uv run --project packages/agent-runtime/examples/python --locked python -m runtime_examples.agent_config
uv run --project packages/agent-runtime/examples/python --locked python -m runtime_examples.loop --tool-decision
```

These commands use the explicitly marked offline provider. For actual model calls,
configure CENTN_PROVIDER_URL / CENTN_PROVIDER_KEY / CENTN_MODEL and add --live. See
`examples/python/README.md` for MCP, Skill, context, history and subagent examples.

## Run a persistent host

Use a dedicated PostgreSQL database/schema and a trusted host-owned identity. Copy
`examples/python/runtime.env.example` to a private path and fill in actual values.
Do not commit that file or supply credentials through browser URLs. Replace the
single-operator example identity/ACL ports for a real multi-user deployment.

```bash
uv sync --project packages/agent-runtime/examples/python --locked --extra host
uv run --project packages/agent-runtime/examples/python --locked --extra host --env-file /private/runtime.env python -m runtime_examples.durable migrate
# Separate long-running processes:
uv run --project packages/agent-runtime/examples/python --locked --extra host --env-file /private/runtime.env python -m runtime_examples.durable worker
uv run --project packages/agent-runtime/examples/python --locked --extra host --env-file /private/runtime.env python -m runtime_examples.durable maintenance
# Only for the delegate profile, using exactly the same configuration:
uv run --project packages/agent-runtime/examples/python --locked --extra host --env-file /private/runtime.env python -m runtime_examples.durable child
```

Run migrations before Workers, and re-run them safely after an interrupted resource
migration. Migrations do not create a database. The SDK does not need knowledge-base
extensions such as vector/zhparser unless your own host's retrieval implementation does.

API acceptance, Worker execution and maintenance recovery are separate lifecycles.
Do not run a persistent Agent as an untracked request-scoped asyncio task. Use
SIGTERM for drain; do not assume a client disconnect cancels a Run. Maintain backups,
restore drills, log retention and a policy for unresolved external effects/token usage.
An unknown effect is not automatically safe to retry or refund.

The standalone Web UI is optional. It can be served by `python -m centn_example api`
after installing its host package. No existing business application is started.

## Validation and operational limits

The current MCP adapter pins protocol 2026-07-28; older initialize-based servers are
not automatically supported. Skill Git sources require a reviewed exact commit,
bounded file hashes, controlled egress and a supervised sandbox. A2A is not certified
by this release. No finite scenario suite guarantees every answer's semantic quality.

Offline CI skips unavailable database/provider integration cases explicitly. A green
offline job does not certify external integrations. The release report separates
current checks from historical real-service evidence and lists unverified boundaries.
Latency depends on model, task, queue and resources; this release makes no universal SLA.
