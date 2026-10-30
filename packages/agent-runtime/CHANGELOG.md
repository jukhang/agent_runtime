# Changelog

## 0.2.1

- Cancel intake updates status/phase/phase timestamp together, so snapshots remain
  valid immediately after accepting cancellation, not only after final convergence.

- Opt-in bounded human waits suspend active execution seconds and atomically restore
  only the remaining seconds on an idempotent reply. Model/tool/token counters remain.
- Admission and Worker hosts configure the same retention policy. Task/grant lifetime
  is explicitly allocated at admission; waits are clamped by that existing grant,
  never silently renewed. Tool/external waits retain existing timeout semantics.
- Existing hosts without this policy retain the previous deadline behavior. No SQL
  schema migration is needed; the optional remainder is in the persisted WaitSpec.
- Real PostgreSQL reply, race, rollback and long-wait tests and offline suite passed.
  Distribution is through GitHub Releases, not PyPI.

## 0.2.0 (Local Upgrade)

- Add the standard `connect_mcp` session adapter backed by the optional official
  MCP Python client, including initialize negotiation, JSON/SSE and session cleanup.
- Default McpClient now expects standard lifecycle support. Existing stateless
  deployments explicitly choose `profile="legacy-discovery"`; no automatic fallback.
- Remove the single-version literal from MCP tool bindings and verify negotiated
  protocol against the reviewed binding at installation.
- Keep MCP ToolEffect independent of SQL storage; the old storage import remains
  an explicit re-export. No database migration or automatic write retry is added.
- Verify multiple protocol versions, incompatible pins, tool error propagation,
  explicit allowlists, closed sessions and credential/redirect restrictions.

This wheel is built and installed locally, not yet published to GitHub or PyPI.

## 0.1.1

- Add backend-only Python tutorials for Agent, Tool, Context, model decisions, MCP,
  Skill, human interaction, subagents and independent SQL Worker clients.
- Add a dark standalone chat UI with left/right messages, IME-aware Enter sending,
  collapsible authorized analysis and incremental Journal output.
- Drain both client tasks on stream failures before releasing host resources.
- Accept already-current migrations and continue host resource migrations.
- Restore committed UI status after a rejected submission; preserve retry identity.
- Add Apache-2.0 licensing, reproducible release checks and independent packaging.

The Runtime core and migration chain are unchanged from the accepted 0.1.0 snapshot.
This release does not certify arbitrary MCP protocol versions, third-party Skills,
or A2A. It does not migrate an existing application automatically.

## 0.1.0

Initial locally accepted SDK baseline: SQL admission, fenced Workers, Journal/Outbox,
Context snapshots, bounded model decisions, ToolHost, approvals, subagents, external
waits and supervised execution adapters. This historical release was not uploaded
to PyPI or to a public GitHub repository.
