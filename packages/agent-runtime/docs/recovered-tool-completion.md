# Recovered Tool Failure Completion

## Transactional rejection and optional compression (2026-11-08)

Transactional handlers may raise DatabaseToolRejected with a typed public failure.
SqlToolHost claims the operation in the outer fenced transaction and executes all
domain writes and output validation inside a savepoint. Only that typed rejection,
after a successful savepoint rollback, produces failed/rolled_back and a durable
failed_safe operation with the exact outcome. Replays return the same rejection,
never re-run its writes. Unknown I/O, cancellation, timeout and commit failures
propagate; the adapter must not manufacture proof for legacy missing operations.
Database capacity is reserved and released in the same outer effect transaction,
for successes and safe failures. Whole-transaction rollback removes its reservation.
There is no separate post-commit slot-release window or guessed reservation lookup.

Verified extraction that does not shrink is a typed no-gain result. ContextRuntime
may omit that automatically loaded optional history, recording degradation; it
must not omit required evidence, swallow malformed citations or provider errors.
No-gain candidates remain in the model audit. A genuinely smaller extraction
that still does not fit is omitted under the same optional-only rule. Required
overflow remains an explicit failure. Existing stored extraction formats remain
readable and verifiable; ordinary responses do not gain an extra model round trip.

MainAgentProfile.allow_user_questions defaults to true. Hosts can explicitly
exclude the ask-user control for autonomous validation/background profiles. This
changes only advertised control capabilities, not cancellation, pause or HITL
authorization. It prevents an internal reviewer from asking the user for ledger
identities or mixing a question with a report submission. Required evidence and
invalid structured decisions are still rejected; this is not a bypass of validation.

Status: ACCEPTED for this scoped change, 2026-11-08. Generic SDK completion,
without document business rules. This is not whole-SDK release approval.

The default on_response path still commits a sealed answer and a terminal event
without an extra model request when there are no unsuccessful tools. Historical
failed/cancelled/not_executed calls are not sufficient evidence of an incomplete
task: the loop may have corrected arguments, used an authorized alternative, and
verified a successful result. Their failure audit must nevertheless remain intact.

When an on_response answer has unsuccessful tool calls, publish the answer and
queue the existing durable completion review rather than prematurely committing
partial. This exceptional review must cover every unsuccessful call. Finish may
include recoveredToolFailures: each entry binds one failed call, later completed
recovery/verification calls, and the reviewer's semantic explanation. The model
judges whether these results satisfy the original goal; the state transaction
verifies exact identities, same Run/scope, durable outcomes, causal order and safe
operation states. An unrelated successful call alone is not semantic recovery.

Success requires all unsuccessful calls accounted for by valid recovery evidence.
Missing coverage remains partial. Invented/cross-Run/duplicate/unfinished evidence
is rejected; unknown external effects or unreconciled operations cannot be waived
by a model. Existing child, pending input, lease/fencing, output seal, resource ACL
and process-stop guards remain mandatory. Failed/cancelled plan steps still mean
partial. Original tool rows/failures are never rewritten to completed.

Recovery assessments persist in the immutable Finish decision/checkpoint committed
with the terminal transition, not in a process-local flag or an ad-hoc database
update. Takeover reuses the staged decision and sealed answer. Ordinary responses
remain one model call; only a failure review or explicit model_review adds one.

Required checks: repaired read and database writes; multiple/unresolved failures;
forged/cross-scope evidence; older success; unknown operation/effect; active child
or execution; rollback and takeover; unchanged no-failure fast path. Real Provider
and installed-wheel host checks are separate from deterministic SQL fixtures.

## Verification

- SDK 0.2.8: 1495 offline tests passed, 436 external-service cases skipped;
  separately selected real PostgreSQL recovery/answer transaction matrix: 51 passed.
  Strict mypy passed for seven changed runtime modules. Ruff passed after test formatting.
- The matrix covers read and committed-write recovery, unresolved failures, foreign
  Run/scope, older success, missing/corrupt outcome and unknown operation effects.
  Existing live-work, rollback and takeover checks remain in the selected suite.
- A real browser + DeepSeek + Docker run produced two failed Python executions,
  three successful executions and a persisted Finish covering both failures:
  completed/success. Original failed calls and errors remain unchanged.
- A separate real missing-dependency script, with repair/retry explicitly forbidden,
  retained its failure and ended completed/partial. Snapshot and browser replay
  explicitly display partial completion. No terminal database repair was performed.
- The two real sandbox runs executed with 0.2.7. The 0.2.8 additive change exposes
  persisted outcome in RunView; completion implementation is unchanged. Its full
  offline/selected SQL checks and repository-external wheel install passed separately.
- Local wheel SHA-256:
  9c0adf05c55f45e1fd13ce51ba4cacd916917d5b924952967bd5b63e893e7556.
  No GitHub/PyPI release is asserted.

Evidence is under the host's ignored output/scenarios/dw06-closeout directory.
Semantic recovery remains the model's explicit, auditable assessment; SQL proves
provenance/effect boundaries, not arbitrary task correctness. This run does not
certify all Providers, P50/P95, high concurrency, or a complete fault release matrix.
