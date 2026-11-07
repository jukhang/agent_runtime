# Agent Runtime 0.2.5 Release Scope

Target: https://github.com/jukhang/agent_runtime, tag v0.2.5, Apache-2.0.
Distribution: GitHub source, wheel and sdist; not PyPI. Publication is complete
only when the tag, Release attachments and source CI are independently verified.

## Changes Since 0.2.1

- Default `completion_policy="on_response"` commits a validated sealed final response
  without an unconditional model-review call or scheduler continuation. Explicit
  `model_review` remains available. Authorization, fencing, pending input, plan,
  child, tool and execution reconciliation remain terminal-transaction checks.
- Persist root claim/context status and expose read-only Journal/model-audit latency
  milestones, sample counts and nearest-rank P50/P95 without extra model requests.
- Explicit DeepSeek thinking configuration is separate from Qwen configuration.
  Invalid/conflicting options fail explicitly. Empty analysis is permitted: the SDK
  preserves actual provider output and does not manufacture thinking text.
- Opt-in native tool history reconstructs same-attempt analysis/content from sealed,
  authorized evidence, groups parallel tool calls and preserves the stored business
  ToolTranscript wire shape.
- Keep standard optional MCP negotiation and existing bounded durable human waits.
  No storage migration or automatic retry of write effects is introduced.
- Allow explicitly declared, disjoint host-owned relations during storage preflight;
  Runtime relation ownership and overlap rejection remain enforced.
- Refresh both example lockfiles to the released SDK version for clean locked installs.

## Verification Scope

Release preparation repeats offline tests, strict typing, lint, Schema/storage
drift checks, standalone/Python examples, selected isolated PostgreSQL tests,
wheel/sdist construction and installation outside the source repository.
The attached verification report records exact counts, skips and source identity.
Skipped external tests are not passes; previously measured application/browser
latency and real-model/sandbox results are supplemental, not rerun release gates.

The default response policy intentionally removes unconditional model verification.
Hosts requiring it must select `model_review` on their installed Agent. The optional
native tool history port requires authorized sealed model evidence; capability
references alone never authorize resource access or tool execution.

## Boundaries

Only the SDK, examples, explicit test helpers and reviewed documentation are public.
No business application, environments, credentials, conversation logs or local test
artifacts are included. Full thinking is protected task data, not public telemetry.

This increment does not certify A2A, arbitrary MCP implementations, all historical
Provider/Skill/sandbox combinations, multi-user host authentication or sandbox
security. Declarative custom Pydantic Agent input/output still requires host
binding; internal decision/tool schemas are not that complete public convenience API.
The final GitHub CI covers offline and PostgreSQL matrices, not live model services.
