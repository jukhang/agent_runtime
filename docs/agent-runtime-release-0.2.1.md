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

## Publication Record (2026-11-02)

Status: PUBLISHED. This post-publication record is maintained in the host repository;
it does not change the tagged SDK source snapshot.

- Release: https://github.com/jukhang/agent_runtime/releases/tag/v0.2.1
- Source commit: `1c707469f378bd3a020293d42c9d0bc17c1f90ab`.
- GitHub CI: https://github.com/jukhang/agent_runtime/actions/runs/41043010732
  (offline and PostgreSQL jobs both succeeded).
- Independent source verification: 1450 offline tests passed, 398 external-service
  tests skipped; 8 selected real PostgreSQL tests passed; 24 Python example tests,
  16 focused standalone host tests and 19 Node tests passed.
- Ruff, strict mypy (226 source files), Schema/storage drift checks, wheel/sdist
  builds and outside-repository wheel installation verification passed.
- Five public attachments were downloaded without authentication and verified
  against local SHA-256 and GitHub asset digests. The downloaded checksum manifest
  also verified all four payload attachments.
- Published wheel SHA-256:
  `ee42b411e8b65e3d6c935ddd1f753907bdf1b67492e6ff2e1ad91fc6c2c61849`.

Distribution is GitHub Release under Apache-2.0, not PyPI. Skipped external-service
tests remain outside this release's acceptance evidence.
