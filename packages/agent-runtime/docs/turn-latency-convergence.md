# Turn Latency Convergence

Default root completion is on_response: a validated native Respond publishes/seals
the answer and commits the terminal state without requeueing or another model call.
model_review is explicit for hosts requiring model verification. Tool execution and
recovery stay behind existing ports; no model request holds a SQL transaction.
Message completion is not Turn completion. The client must await the durable Run
terminal event. All existing lease, pending input, live child, execution, authorization
and output seal checks remain mandatory. Unsuccessful tools trigger a durable
completion review: success requires persisted and verified recovery evidence for
every failure; unresolved failures remain partial. The failure audit is preserved.
An unfinished persisted Plan continues through the normal loop rather than being
silently completed by Respond. Closed failed/cancelled steps yield partial.

Measure acceptance, claim, context/model preparation, first analysis, first content,
last content, message completion and terminal event separately. Report P50/P95 with
sample counts, model/profile/concurrency and failures; excluded failures must be named.
Real acceptance includes ordinary conversation, thinking, tool execution and concurrency.
Small-sample percentiles are diagnostic, not a production latency guarantee.

## Host Configuration

`Agent(completion_policy="on_response")` is the default. A root native response
finishes only after its normal validated Decision, sealed output and SQL authority
checks. `Agent(completion_policy="model_review")` retains the explicit continuation
and verification call for domain review. The policy is part of an immutable Agent
manifest: install a new revision/identity, drain incompatible old Runs, and do not
silently replace their configuration. No provider stop token alone ends a Run.

Root Worker loading records `run.status(running, context)` using the existing SQL
transaction and claim epoch as its dedup identity. It does not increment business
state_version or replace the checkpoint. The event occurrence timestamp comes from
the persisted claim phase timestamp, not browser receipt or local playback.

## Measurement Contract

`agent_runtime.latency.measure_turn(events, run_id)` projects occurrence timestamps
from a Run's authoritative events, sorted by session sequence and deduplicated by
event ID. It does not write to storage or call a model. Feed restricted
`model.prepared` events only from a trusted operator path to count calls. Missing
milestones remain null; do not interpret zero prepared events in a public-only
stream as proof of zero model calls.

Metrics include acceptance-to-running, running-to-first-model-prepared,
acceptance-to-first-analysis/content, model-prepared-to-first-output, last-content
to-message-completion, message-completion-to-Run-terminal and total time. Context
preparation is an aggregate before the first request, not a claimed independent
measurement of every context subcomponent. Client HTTP admission and SSE receipt
times are recorded separately by the host harness.

`percentiles` uses nearest rank and exposes n. The standalone host command is:

```bash
uv run --project backend --locked --env-file .env python scripts/verify_turn_latency.py \
  --samples 20 --concurrency 4 --output output/latency/sdk-0.2.3
```

Run from centn-agent. It creates fresh sessions for ordinary QA, thinking, Python
sandbox execution and four-concurrent QA, and stores per-Run evidence. Check a
single model attempt for ordinary QA, visible thinking when requested, actual tools,
terminal success and no queued status after final content. It does not delete user
data or change host configuration. A small run is diagnostic, not a production P95.

## Historical Candidate Verification (0.2.3, 2026-11-07)

- SDK offline suite: 1464 passed, 417 external-service tests skipped.
- Original remote PostgreSQL isolated answer matrix: 28 passed, before the root
  claim event addition. Later local PostgreSQL verification is recorded separately.
- Final local Docker PostgreSQL (aletai_wangxh, random isolated schemas): 39 passed,
  including root loader claim-event dedup/fencing, default single-model completion
  with no continuation, explicit two-model review, retry output isolation, rollback,
  unfinished-plan continuation, failed-plan partial outcome, SDK ingress, scope
  checks, snapshot and Journal replay.
- Seven changed SDK modules pass mypy; changed files pass Ruff.
- Installed wheel host: 224 backend tests and 58 Web tests passed.
- Wheel 0.2.3 built and installed from a local artifact, not publicly released.
- Real HTTP latency matrix attempted but BLOCKED: configured MCP connection timed
  out; a subsequent test startup without that endpoint still failed because
  192.168.1.94:5432 was unreachable. Qwen at 192.168.1.94:9919 also timed out.
  No real P50/P95 or first-token improvement is claimed from this run.
- Application .env/database target was not changed. A test-only temporary empty
  MCP endpoint was attempted; this is not MCP acceptance or a production fallback.

At that snapshot acceptance remained IN_PROGRESS until real QA/thinking/tool/concurrency measurements
are collected and reviewed. A wheel installation alone does not close this gate.

Subsequent host configuration: the user selected local Docker PostgreSQL and the
DeepSeek official endpoint/model deepseek-flash. Local schema migration succeeded;
the supplied online credential returned HTTP 401. Real Provider measurement remains
blocked by authentication. Generic transport does not certify DeepSeek-specific
thinking control or reasoning history pairing; those need separate adapter tests.

Credential follow-up: the replacement official key passed model discovery and a
minimal non-thinking deepseek-flash completion (HTTP 200, finish_reason=stop).
Authentication is no longer blocked. Full SDK scenario/latency acceptance is still
pending, including explicit DeepSeek thinking control and tool-history verification.

## DeepSeek Provider Acceptance

Use an explicit deepseek adapter profile: map ModelConfig.thinking to the native
thinking.type request field, reject conflicting/foreign thinking options, and
preserve normalized reasoning_content in the analysis channel. A generic profile
must not claim to control thinking. Provider connection and secrets remain host-owned.
For tool continuation the SQL source reader may explicitly include native model
history from the same call's sealed model attempt, after normal source ACL checks.
Completed tool-only attempts with no analysis/content events have no text producer
to seal. They retain call/result/attempt identity without manufactured text; any
existing text still requires sealed evidence. Bind lookup to the authorized source
Run, which need not be the consuming Run.
Group calls produced by one attempt into one assistant message, preserve its exact
analysis/content and pair all tool results. Never invent blank reasoning to replace
missing persisted data, and never export that history into public browser events.
This is provider protocol data inside authorized context snapshots, not a business
rule or a second, untracked prompt/history source.

The final candidate uses a separate NativeToolTranscript enrichment, leaving the
stored ToolTranscript/effect shape unchanged. SQL ModelAttemptTiming supplies
attempt identity, context creation and request start times when a path has no
model.prepared event. Never interpret missing events as zero actual model calls.
The phase metric named model_first_output_ms is measured from audited request start,
not context preparation. The performance harness separately records concurrency for
each group and uses actual completed Tool events plus expected result checks.

## Current Verification (0.2.5, 2026-11-07)

Core latency goals pass in the configured local host. This is not a public release
approval or certification of every Provider/domain combination. Full supplemental
thinking-with-tools visibility acceptance remains IN_PROGRESS (details below).

- SDK offline: 1472 passed, 417 external-service skips. Changed modules pass Ruff
  and strict mypy (5 modules); the installed wheel matches source byte-for-byte.
- Local PostgreSQL isolated context/answer/Worker/SDK recovery matrix: 40 passed.
- Installed-wheel host: 225 backend tests; Web 58 tests and production build pass.
  Existing large JavaScript chunk warning remains unrelated.
- Repository-external base wheel and storage/provider/example-host installation pass.
- Wheel SHA-256: 187adae56ce491771ff43618409ba1b4517443c35efb5f9258b742d9cd1ce43c.
- Actual API + independent Worker + maintenance + Docker sandbox + deepseek-flash:
  five groups of 20 Runs, all 100 task outcomes successful, no queued state after
  final content. QA/thinking/concurrent use one model call each; tool groups two.
- SQL closeout: 100 completed Runs, 140 completed attempts, 40 completed ToolCalls,
  40 completed Executions, 40 committed Operations, 180 completed WorkItems;
  no unsettled budget entries or unknown budget usage for those Runs.

Nearest-rank server occurrence-time metrics (milliseconds; each row n=20):

| Group | Concurrency | First output P50/P95 | Message-to-terminal P50/P95 | Total P50/P95 |
| --- | --- | --- | --- | --- |
| QA | 1 | 1450 / 1729 | 46 / 69 | 1970 / 2364 |
| Thinking QA | 1 | 1431 / 1878 | 48 / 84 | 2266 / 3266 |
| Sandbox tool | 2 | 1293 / 1623 | 40 / 58 | 25469 / 27790 |
| Thinking + sandbox | 2 | 1272 / 1511 | 34 / 61 | 24827 / 26268 |
| Concurrent QA | 4 | 1570 / 2061 | 61 / 92 | 2637 / 3292 |

First output means first persisted content/analysis delta, not raw inference TTFT
or browser paint. Client request-to-first-SSE-output P50/P95 respectively: QA
1662/1968 ms, thinking 1683/2066 ms, tool 1447/1717 ms, thinking-tool 1368/1655 ms,
four-concurrent QA 1699/2275 ms. No same-Provider pre-change baseline was collected,
so these numbers do not prove a numeric before/after speedup. Early samples overlap
the isolated SQL regression workload; these are observed acceptance measurements,
not a quiet-environment SLO or a production capacity guarantee.

The four required groups pass all configured checks (80/80). Supplemental
thinking-tool is 14/20 under the additional visible-analysis check: six completed
correct tool executions have no analysis.delta in SQL or SSE. Their metrics stay
in the denominator; original failed checks are not overwritten. A direct official
thinking/tool probe returned analysis, but does not prove what those six original
raw streams contained. Do not assume the provider or parser is at fault without
outbound-mode/raw-stream correlation. Keep this visibility gate open; never invent
analysis or silently change the acceptance condition to report 100/100 checks.

Raw host evidence: centn-agent/output/latency/sdk-0.2.5-final/ (100 individual
records + summary.json, ignored local artifacts). Completed-history browser
verification: output/playwright/deepseek-completed.png. Remote office config remains
commented in the ignored .env; application data was not reset.

## Visibility Investigation Follow-up

The supplemental gate above is closed as a reproduced provider variation and an
invalid nonempty-reasoning assumption. Replayed all six original snapshots three
times: 18 enabled native-tool requests, two raw empty-analysis responses, 18/18
raw/normalized character-count agreement, no parser errors. Original raw streams
were not retained, so this does not claim byte-for-byte reconstruction of the six
historical requests. The previous failure records remain intact.

The host now tests pinned model mode plus exact SQL/SSE analysis fidelity. Empty
analysis is an observation, not automatically a failure; lost, changed or invented
analysis still fails. Extra model rounds must correspond to real tool-producing
decisions in these QA/tool scenarios, not an unconditional Judge call. A new
10-Run functional check and read-only SQL reassessment satisfy that rule. It is
not substituted for the original n=20 performance samples.

See centn-agent/docs/deepseek-thinking-visibility-investigation.md and ignored
output/latency/thinking-investigation/ evidence. SDK production stream code and
the installed 0.2.5 wheel were not modified; null/empty reasoning tool-stream
regression cases were added instead. This is not a public release approval.
Final investigation regression: SDK 1474 passed / 417 external-service skips;
host 228 passed; changed diagnostic scripts and tests pass Ruff.
