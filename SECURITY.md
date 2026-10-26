# Security

Do not report credentials, private prompts, customer data or a working exploit in a
public issue. Use GitHub's private vulnerability reporting when enabled, or contact
the repository maintainer privately. If no private channel is available, ask for one
without disclosing the vulnerability details. No response-time SLA is guaranteed.

## Deployment boundaries

- Models and generated code are untrusted. Never execute Skill code in the API or
  Worker host. Use a supervised sandbox with hard resource limits and controlled egress.
- Keep provider/MCP/Git credentials in host-owned credential ports, never in Agent
  configuration, prompts, browser URLs, sandbox files or public logs.
- Agent capability references do not grant authority. Install pinned revisions,
  register handlers and enforce current resource ACL/approval checks at execution.
- Runtime schema migrations need explicit deployment ownership. Use independent
  Worker/maintenance processes; do not treat a browser connection as execution ownership.
- The standalone host uses a single-operator development token. Replace it with real
  authenticated identity/authorization ports for multi-user production deployment.
- Full analysis can contain private task data. Enable it only for authorized readers.

The current MCP adapter pins a specific protocol revision; unsupported servers must
fail explicitly. The project does not claim all remote tools, Skills or models are safe.
