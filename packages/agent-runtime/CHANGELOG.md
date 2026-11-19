# Changelog

## 0.2.20

- Promote the SDK runtime code from 0.2.20rc20. The Python example now targets
  the stable version, and `[storage]` includes the HTTP client required by
  the SQL Worker assembly without requiring an explicit `[provider]` extra.
- Bound model working sets and recover from provider and tool failures without
  treating incomplete streams or unverified effects as successful.
- Reserve and settle finalization budget for root and child Runs; validate
  terminal evidence before reporting a task complete.
- Negotiate supported MCP protocol versions rather than requiring one specific
  server version; preserve host authorization and effect recovery boundaries.
- Back up the Runtime schema and drain active runs before upgrading a host.
  The release adds no SDK schema migration; do not assume unfinished Runs
  can be moved between arbitrary SDK/host versions.
- Published on GitHub under Apache-2.0; no PyPI release or A2A certification.

## 0.2.20rc20 (Local Candidate)

- Preserve the first Worker/recovery failure even when sibling cleanup also
  fails. Keep the original traceback and await both roles before propagating.
- Supersedes the unpublished rc19 acceptance build; not a stable release.

## 0.2.20rc19 (Local Candidate)

- Add optional require-any-native-tool selection without an extra model call;
  preserve immutable tool bindings, reject unknown tools and truncated output.
- Preserve the original Worker failure traceback while draining its sibling so
  hosts can classify transient database failures without blanket retries.
- Unpublished candidate; long-document and browser acceptance are not complete.

## 0.2.20rc18 (Local Candidate)

- Narrow completion recovery references by current SQL status and delivered
  Snapshot identities in the dynamic output schema. Failed and successful call
  menus are separate; these menus do not certify recovery or replace the final
  evidence, ordering, scope and effect-safety checks.
- Expose the same schema binder to application-owned Worker composition without
  adding a model round trip. Unpublished candidate pending product acceptance.

## 0.2.20rc17 (Local Candidate)

- Include bounded, delivered-only recovery identity hints for every invalid
  completion-evidence candidate, not just missing coverage. Preserve correction
  limits, intact audit checks, effect reconciliation and final SQL authorization.
- Freeze the finalization and independent-process regression fixtures together;
  this candidate remains unpublished pending browser product acceptance.

## 0.2.20rc16 (Local Candidate)

- Share finalization-purpose budget selection between root and child decision
  loops; preserve the SQL claim, usage and authorization boundaries.
- Add SQL coverage for actual child closing, process death with independent
  maintenance recovery, and real TCP unavailability through the root Worker.
- This is an unpublished acceptance candidate, not a stable release.

## 0.2.20rc15 (Local Candidate)

- Freeze the explicit named-tool capability without changing positional adapter
  arguments. Supersedes the unpublished rc14 build; not a public release.

## 0.2.20rc14 (Local Candidate)

- Add explicit `OpenAIAdapterProfile.supports_forced_tool_choice` (default true)
  for private endpoints with incompatible named-function decoding. Auto wire
  selection preserves the immutable required-tool binding and fail-closed Loop
  validation; it is not automatic fallback or permission relaxation.
- S02/S03 integration candidate only, not a public release.

## 0.2.20rc13 (Local Candidate)

- Freeze the completion-coverage correction with lint-clean source formatting;
  feedback semantics and limits are unchanged from rc12. Not a public release.

## 0.2.20rc12 (Local Candidate)

- Keep the complete bounded failure identity list in completion correction
  feedback so incremental fixes cannot silently discard previous assessments.
  Existing retry limits, immutable audit and final commit checks are unchanged.

## 0.2.20rc11 (Local Candidate)

- Reject successful completion candidates with incomplete tool recovery coverage
  before staging, allowing bounded durable correction instead of silently
  downgrading at commit. Explicit partial outcomes and final SQL provenance,
  effect and authorization checks remain intact. Not a public release.

## 0.2.20rc10 (Local Candidate)

- Expose bounded candidate repair on the per-claim MainAgentProfile, disabled by
  default and on control lanes. Reviewer hosts can install the same durable
  repair path without copying the Loop or weakening final evidence validation.
- Product and independent-process acceptance remain outstanding; not a release.

## 0.2.20rc9 (Local Candidate)

- Persist typed rejected-candidate observations with their checkpoint CAS; fail
  closed on missing history and retain correction bounds across worker takeover.
- Protect an opt-in closing model/token window through admission and actual
  attempt transactions. Atomically claim once, preserve unknown usage, release
  unused holds at terminal transitions, and keep child allocations isolated.
- Use a compact no-tool closing context and reserve the last bounded runner
  iteration; ordinary successful responses add no model round trip.
- Classify invalid completion references without granting new authorization;
  distinguish retained output records from saved domain artifacts in guidance.
- Reject bare native tool envelopes returned as answer text through the opt-in
  bounded candidate correction path; never execute content as a tool call.
- Reuse existing SQL ledger columns; no DDL migration or public release claimed.
- A fenced model attempt cannot silently fall back to unfunded audit-only mode
  when its budget allocation is missing.
- Browser product acceptance remains outstanding; this is not DW-06 acceptance.

## 0.2.20rc1 (Local Candidate)

- Add trusted `DecisionSetup.observation_policy="isolate"` for independent model
  stages. Default observation inclusion and cross-scope rejection remain unchanged.
- Context conflict refresh cannot change the stage's observation policy.
- This is a local integration candidate, not a stable release or a GitHub publication.

## 0.2.19 (Local Validation Candidate)

- Reconstruct display snapshots in bounded SQL sequence/byte pages at one MVCC
  watermark, instead of rejecting a long Journal whose final projection fits.
- Persist restricted display projections atomically and monotonically; reuse
  their watermark, never replace them from public-only reads, and reauthorize
  before writing. Unchanged snapshots do not churn projection revisions.
- Report actual oversized snapshots/events as `SNAPSHOT_TOO_LARGE`; preserve
  authorization, retained-prefix and integrity checks. No history is deleted.
- Bound accumulated view data after each replay page as well as final wire and
  cached representations. 0.2.18 was an intermediate local validation wheel;
  its installed bytes are not overwritten under the same version.
- This local candidate is not a GitHub release or DW-06 acceptance.

## 0.2.17 (Local Validation Candidate)

0.2.14–0.2.16 were intermediate local installations, not public releases.

- Compare compression candidates with the same request estimator, including summary
  framing. Persist run-scoped verified no-gain observations to avoid repeating the
  same compression after restart. Invalid citations still fail explicitly.
- Add opt-in ToolResultWindow policies: retain the latest successful read in full,
  offer older immutable results as retrievable indexes only when required input
  would overflow. Unregistered results and failed effects cannot be discarded.
- Persist private structural stream diagnostics through an injected sink; allow
  bounded model recovery for malformed events, draining accounting frames without
  executing damaged candidates. Empty tool identity continuations are tolerated;
  conflicting nonempty identities and invalid JSON remain rejected.
- No schema migration, historical state rewrite or public release is asserted.

## 0.2.13 (Local Validation Candidate)

- Commit transactional database capacity reservation/release in the same SQL
  transaction as the Operation outcome. Rollback removes both the capacity intent
  and domain changes; safe rejection commits the failed_safe receipt with no live
  permit. This closes the commit-before-slot-release crash window for database
  tools without guessing which random reservation belongs to an operation.
- Includes 0.2.12 recovery/context fixes. No public release is asserted.

## 0.2.12 (Local Validation Candidate)

- Persist safe database rejections with their Operation identity: outer ledger claim,
  domain savepoint rollback, failed_safe receipt and idempotent rejection replay.
  Unknown I/O, timeout, cancellation and failed transaction exits still propagate.
- Release database capacity for safe failures and settle lost release receipts on replay.
- Treat a valid optional extraction with no size reduction as no-gain, not a fatal
  Run error; record omission without evicting required facts or accepting bad citations.
- Let hosts disable user-question controls for autonomous validation profiles.
  Interactive agents retain their default clarification capability.

0.2.10/0.2.11 were local intermediate validation wheels, not public releases.
No schema migration or rewrite of historical failures is performed.

## 0.2.9 (Local Validation Candidate)

- Reconcile a not-executed, zero-effect tool outcome's planned operation identity
  without requiring an Operation row for an invocation rejected before execution.
  Require the immutable failure to bind the same planned identity. Executed/unknown
  effects retain the existing ledger requirements; original failures remain intact.

## 0.2.8 (Local Validation Candidate)

- Expose the persisted completion outcome in Run snapshots so partial completion
  remains distinguishable from success after refresh and replay.
- Include the recovered-tool completion behavior from the immutable 0.2.7 local
  candidate. No migration and no public release claimed by this validation.

## 0.2.7 (Local Validation Candidate)

- Keep the no-failure on-response fast path. An unsuccessful tool invokes a durable
  completion review rather than immediately forcing a partial terminal state.
- Persist per-failure recovery evidence in Finish. Verify same-Run context-bound
  tool outcomes, causal ordering and reconciled effects before successful completion.
  Unresolved failures remain partial; original failed tool audit is never rewritten.

## 0.2.6 (Local Validation Candidate)

- Accept completed tool-only model attempts with no text producer in opt-in native
  tool history. Reject existing unsealed text and preserve source ACL/version checks.
- Bind history enrichment to the authorized source Run, not the consuming Run.
- DeepSeek uses explicit JSON-object mode with the bound schema instruction for
  structured responses; Qwen retains JSON Schema mode. Local validation is unchanged.
- Preserve DeepSeek thinking with automatic native tool choice where forced named
  choice is unsupported; AgentLoop still requires the exact bound tool before commit.
- No migration. The existing published 0.2.5 artifacts remain immutable.

## 0.2.5

- Add explicit DeepSeek thinking.type mapping and reject mismatched/foreign thinking
  options. Keep Qwen chat_template_kwargs handling separate.
- Opt-in SQL native tool history derives exact analysis/content from the same
  sealed model attempt after normal resource authorization. Group parallel calls
  into one assistant message with paired results; reject conflicting history.
- Preserve the existing stored ToolTranscript/effect wire shape. Provider history
  is a separate NativeToolTranscript enrichment, not nullable fields on business results.
- Latency projection accepts run-bound SQL ModelAttemptTiming audit facts when
  model.prepared events are absent, avoiding false zero-call reports. No model
  request is added for measurement.
- Includes the on-response completion and explicit review policies from 0.2.3.
  Publication checks and remaining integration limits are recorded in the release report.

## 0.2.3 (Local Validation Candidate)

- Root Agents complete a validated, sealed native response without an unconditional
  model-review call or another scheduler continuation. Hosts requiring a model
  verifier explicitly select `completion_policy="model_review"`.
- Keep live ownership, authorization, pending input, child collection, tool/execution
  reconciliation and sealed output checks on the terminal transaction. Failed tool
  attempts conservatively yield partial completion, not unconditional success.
- Publish root claim/context status durably from the existing loader transaction.
  Add read-only Journal latency projection and nearest-rank P50/P95 with sample counts.
- No database schema change. Real Provider percentiles remain a separate required
  measurement, not something certified by offline tests or this version number.

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
