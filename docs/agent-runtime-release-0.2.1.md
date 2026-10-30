# Agent Runtime 0.2.1 Release

Target: https://github.com/jukhang/agent_runtime, tag v0.2.1, Apache-2.0.
This source document defines release scope; actual publication and CI status must
be verified against the GitHub tag/Release, not inferred from this document.

## Changes Since 0.1.1

- Standard MCP lifecycle, negotiated supported protocol versions, JSON/SSE/session
  handling and optional reviewed version pins through the official optional client.
  Legacy stateless discovery is explicit, never a silent fallback or downgrade.
- Host-opt-in bounded human waiting: persist remaining active execution seconds,
  atomically restore them on reply and preserve counters. Task/grant retention is
  allocated at admission; waits never extend an existing authorization grant.
- Atomic cancel status/phase/timestamp updates keep intermediate snapshots valid.
- Additional MCP, human reply, concurrency, rollback and cancellation regression tests.

## Verification And Boundaries

Prior local verification: SDK offline 1450 passed, 273 external-service skips
(excluding the separately exercised SQL UoW file). Seven real PostgreSQL human
reply/wait scenarios and an additional immediate cancel-snapshot scenario passed;
the grant-clamped long-wait scenario was rerun. Independent application Qwen/control/
review tests passed, but its private source, data and logs are not part of this release.
The release preparation independently repeats Ruff, strict typing, Schema/storage
checks, offline/example tests, clean package installation and selected real SQL tests.
Published verification assets must state exact observed counts and skipped scope.

No SQL schema migration is introduced by this increment. Configure the same
human_wait_seconds on admission and Worker setup to enable suspended active time.
Existing deployments without this policy preserve previous deadline semantics;
old Runs do not receive retroactively renewed grants. MCP requires its optional
extra; standard session initialization is a deliberate change from 0.1.1's default
legacy discovery transport. Consult docs/mcp-compatibility.md inside the SDK.

No claim of arbitrary MCP compatibility, A2A completion, production multi-user
identity, full sandbox security certification or all historical provider/sandbox
scenarios being rerun. Offline skips are never counted as external passes.

Only the independently exportable SDK, examples, necessary test helpers and explicit
documentation enter the public repository. No original business history, app/web,
centn-agent, private environment, credentials, raw conversation or diagnostic log
is uploaded. GitHub assets contain wheel/sdist, redacted verification and checksums.
