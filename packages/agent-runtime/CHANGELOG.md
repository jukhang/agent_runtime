# Changelog

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
