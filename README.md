# centn Agent Runtime

An independent Python Agent Runtime SDK for developer-owned applications.
Licensed under Apache-2.0. The SDK and examples live at `packages/agent-runtime`.
This repository does not include the original business application or its Git history.

## Start Here

```bash
uv sync --project packages/agent-runtime --locked
uv run --project packages/agent-runtime --locked python -m pytest packages/agent-runtime/tests -q
uv sync --project packages/agent-runtime/examples/python --locked
cd packages/agent-runtime/examples/python
uv run --locked python -m runtime_examples.loop --tool-decision
```

The last command is an explicitly offline protocol example, not a real model call.
See the [SDK README](packages/agent-runtime/README.md),
[Python examples](packages/agent-runtime/examples/python/README.md),
[standalone UI](packages/agent-runtime/examples/standalone/README.md),
[deployment](packages/agent-runtime/DEPLOYMENT.md),
[changes](packages/agent-runtime/CHANGELOG.md), and
[release scope](docs/agent-runtime-release-0.1.1.md).

Download signed-off local wheels and SHA256SUMS from GitHub Releases. This project
does not publish to PyPI in this release. Capability references alone never authorize
tools; hosts own identity, ACL, grants, connections and independent Worker processes.
PostgreSQL, providers, MCP endpoints and sandbox services need explicit configuration.
The example identity is single-operator development authentication, not a production
multi-user login system. Full analysis is restricted task data, not public telemetry.

Offline skips are not external integration passes. The release report distinguishes
current checks from historical real-service evidence. Current MCP support pins
2026-07-28; arbitrary protocol compatibility, A2A and existing product migration are
not certified. See [Security](SECURITY.md) before deployment.
