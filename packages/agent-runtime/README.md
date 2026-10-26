# Agent Runtime

Version 0.1.1 is distributed under Apache-2.0. See [CHANGELOG.md](CHANGELOG.md),
[DEPLOYMENT.md](DEPLOYMENT.md), [SECURITY.md](SECURITY.md) and [LICENSE](LICENSE).
GitHub source is kept at `packages/agent-runtime` inside the independent repository;
examples and their local SDK paths keep the same layout.

Reusable runtime package for centn and independent hosts. The core package provides
configuration, event, snapshot and request/result contracts, Provider invocation
convergence and the scheduler/worker interfaces. Optional adapters remain isolated:
``provider`` adds the OpenAI-compatible HTTP adapter and ``storage`` adds the standard
PostgreSQL models, migrations and durable stores. Importing the core starts no service,
opens no connection and reads no host configuration.

## Independent host integration

For backend-only Python walkthroughs (Agent, Tool, Context, model decisions, MCP,
Skill, human interaction, subagents and independent SQL Workers), see
[`examples/python/README.md`](examples/python/README.md). They do not start a Web UI.

`Runtime(admission=..., backend=...)` is the SDK entry point. `submit(actor, request)`
commits Admission and returns `SubmittedRun(accepted, handle)`; it does not create an
in-process execution task. An independently deployed Worker advances the run.
`attach(actor, session_id, run_id)` reconnects a client handle without resubmitting.
Each read and command rechecks current authorization; holding a handle is not a grant.

Install `SqlInteractionService` explicitly with `Runtime(..., interactions=...)` to accept
`InteractionRequest` replies to the current `AskUser` wait. Replies bind the Run and wait
version; an exact idempotent retry returns its original receipt. The human Timeline Item,
checkpoint, Journal/Outbox and continuation WorkItem commit atomically. No synthetic
agent sender or API-owned execution task is created.

The SQL display backend defaults to a public view: restricted analysis and its
channel declarations/digests are removed together. A host may configure
`include_analysis=True` only with an authorizer that permits restricted reads; every
page/snapshot rechecks that authorization. This view adds analysis deltas, not private
model-prepared or credential events. It never changes the stored Journal facts.

The optional SQL adapter provides `SqlSessionStore` and `SqlRunBackend` for session
ownership, bounded MVCC snapshots, Journal subscriptions, terminal results and controls.
Host engines/HTTP clients belong to the host lifespan, not to Agent configuration.

The standalone example supplies its own API, Worker, token-authenticated development UI
and opt-in PostgreSQL/Qwen/Chrome tests. See
[`examples/standalone/README.md`](examples/standalone/README.md) for startup. No existing
`app/` or `web/` production route is replaced. Installation alone is not release acceptance:
local release scope, evidence and status are recorded in
`docs/agent-runtime-release-0.1.1.md`. A2A, arbitrary public Skill/MCP certification
and migration of an existing product are separate from this independent SDK delivery.

`SqlRunBackend.materialize(actor, session_id)` saves a versioned, immutable projection
with its exact watermark and authorization revision. Subsequent snapshots restore that
prefix and replay only the retained tail. Missing prefixes, corrupt blobs, unsupported
projection versions and stale authorization fail closed. Materialization requires restricted
read authorization because the internal checkpoint preserves both output channels; public
reads still remove analysis. Hosts must coordinate execution, audit and replay retention
before deleting Journal history: this method itself never prunes events. Result waiting
and controls authorize the current Run directly and do not rebuild full history.

```python
from agent_runtime import Agent, CapabilityKey, CapabilityRef

agent = Agent(
    id="research",
    name="Research",
    instructions=("Cite the evidence used in your answer.",),
    tools=(
        CapabilityRef(
            key=CapabilityKey(namespace="builtin", key="knowledge.search"), revision="1"
        ),
    ),
    add_datetime_to_context=True,
    timezone="Asia/Shanghai",
)
manifest = agent.model_dump(mode="json", by_alias=True)
```

References are not authorization grants. Installed registry revisions and live
permission ports validate their availability and permissions. No shared user/session state or clients belong in
an Agent. Configuration is immutable, rejects unknown fields, and round-trips
through camelCase JSON. The package does not import the host application or Agno.

From this directory:

```bash
uv sync --locked
uv run --locked pytest
uv run --locked ruff check .
uv run --locked mypy src
uv build
```

Copy the complete package, including its build files, or install its wheel in a
new host. Do not copy isolated source files. The single-step ``ModelAgentLoop`` and
``RunActionDispatcher`` are available, but a host must inject durable decision,
observation, authorization and tool adapters before using them for production side
effects. A standalone reference host and browser reducer now live in
[`examples/standalone`](examples/standalone/README.md); they do not import the existing
application or frontend.

The storage adapter also supplies ``SqlAdmission``, ``SqlAgentInstaller``,
``SqlMainHandlerFactory``, ``SqlClaimMainHandler``, ``WorkerRunner`` and
``SqlWorkerService`` for a durable root loop. The factory reads actual Turn inputs and
bounded history, pins installed manifests and binds each claim's current Actor. Hosts
must still supply trusted model connections, tool definitions/execution, resource access
and policy ports. Unbound Agent knowledge, Skills, Hooks, custom schemas, memory/context
or policy/delegation declarations are rejected rather than silently ignored. The supplied main
setup is a native answer/tool/clarification profile followed by a finish-only review; a full
developer profile can be registered through `MainSetupFactory` and per-profile output
formats. The host must implement and validate each advertised capability; the extension
point alone does not provide a full planner/delegation profile.

`SqlPlanningSetupFactory` supplies an opt-in initial-plan / native execution / plan-review
profile. Plan/step IDs, revisions, sealed final items and exact evidence refs are host-bound;
candidate validation runs before decision/answer persistence even if the Provider ignores
JSON Schema. Optional `PlanningOptionsPort` must return installed child task packages and
durably wakeable waits. Its declaration is not a child Worker or a budget authorization;
hosts must install and test those executors separately. No options means no Delegate/Wait
actions are exposed to the model.

The reference factory's answer-review output permits only finish (success/partial).
Essential clarification is an explicit native Runtime control mapped to `AskUser`, not
an external ToolHost effect. It must be visible in the pinned Context, complete cleanly,
and cannot share a batch with business tools. Its reserved reference cannot be declared
as a host business Tool. Generic plan,
delegate and wait actions remain available through injected ``LoopSetup`` and their SQL
committers; they are not advertised without trusted profile-specific identity bindings.
Answer and tool actions use
their native streaming channels; the structured schema does not advertise those actions.
Admission's ``assistantItemId`` reserves the **first public output**, not necessarily the
final answer after several tool/model steps. Consumers must use the Journal's terminal
item references instead of assuming one assistant item per Turn.

Core acceptance does not switch an existing application's API/UI or remove its workers.
Unified recovery policy, production sandbox/credential/tool adapters and the independent
SDK host/transport/reducer each have separate acceptance gates.

OpenAI-compatible streaming requires the provider extra:

```bash
uv sync --locked --extra provider
```

Construct ``OpenAICompatibleProvider`` with a lifespan-owned ``httpx.AsyncClient`` and
trusted input/connection resolvers. Wrap it with ``ProviderInvoker`` to enforce capability
preflight, absolute deadline, cancellation, stream integrity and one durable ModelAttempt
lifecycle. Only the atomic ``ModelAttemptStore.begin`` winner sends a provider request;
an already-running attempt is fenced. The persisted identity includes a canonical digest
of the full ``ProviderRequest``, including the deadline. The adapter never reads
``.env``, resolves credentials globally or retries internally. Online endpoints use
``OpenAIAdapterProfile``; private Qwen/vLLM endpoints can use
``OpenAIAdapterProfile.qwen()`` so ``chat_template_kwargs.enable_thinking`` stays inside the
adapter rather than leaking into Agent definitions.

The generic profile recognizes structured context-limit error codes only. The Qwen
profile additionally accepts the observed vLLM BadRequest context-limit wire format,
with bounded complete grammar and consistent token arithmetic. Other messages remain
ordinary rejections; this is not keyword-based task routing. Missing usage on a rejected
request remains unknown, never an assumed zero bill.

The normalized stream separates ``analysis.delta`` from ``content.delta``, accumulates
multi-call tool arguments by provider index, and releases complete tool calls only after a
valid ``tool_calls`` finish. A finish choice is not treated as end-of-stream until a trailing
usage frame has been consumed, because OpenAI-compatible servers commonly report Usage in a
separate frame before ``[DONE]``. Missing Usage remains
``unknown/provider_not_reported``; cancellation never invents zero tokens. Length, content
filter, malformed data, missing terminal, timeout and HTTP failures are terminal facts, not
transparent retries.

Standard PostgreSQL storage requires the extra and runs migrations only through a
controlled deployment role::

```bash
uv sync --locked
uv run --locked pytest
uv sync --locked --extra storage
uv run --locked python -m agent_runtime.storage.migrate upgrade --dsn postgresql+asyncpg://…
uv run --locked python -m agent_runtime.storage.migrate check   --dsn postgresql+asyncpg://…
```

``agent_runtime.storage`` exposes ``Base``/``models`` (45 ``*Row`` tables required by the
frozen catalog), ``StorageConfig``/``create_storage_engine``/``create_session_factory``,
``compare_catalog_to_database`` and the migration CLI. The chain owns
``<schema>.alembic_version`` in its own schema, never the host or ``public`` version table,
and each revision runs from its own immutable catalog snapshot, so a later revision or
catalog change never rewrites already-applied DDL. The head revision is
``0008_human_input``, extending the event CHECK for atomic human replies; downgrading
is refused while `input.accepted` facts exist.
``0007_capability_superseded`` adds the registry supersession state.
``0006_credential_service`` adds immutable mTLS service-to-grant bindings.
``0005_credential_ledger`` adds private credential request/audit tables.
``0004_input_required`` extends the event constraint for durable user-input requests.
``0003_context_blob`` added the scope-isolated, immutable ``context_blob`` table.
``0002_evidence_ownership`` adds the ``output_seal`` table, ``run.authority_revision``,
``execution.run_id`` and ``system_capacity_reservation.release_request_hash``;
``0001_initial_storage`` stays byte-frozen. The unreleased 0002 revision backfills execution
ownership only when an existing task has one unambiguous run, restores each run's authority
fence from existing grants, and rejects ambiguous legacy execution ownership instead of
guessing.

Context input uses ``SqlRuntimeSourceReader`` for owner-authorized, pinned goal/plan
revisions and ``BlobContextPayloadStore(SqlContextBlobBackend(...))`` for shared bytes.
``SqlContextSnapshotStore`` refuses to commit a snapshot unless the matching blob exists;
``SqlContextAccessVerifier`` rechecks current Session/Run authority and every included
source before a ``ContextBoundProvider`` opens a model stream. Other resource kinds fail
closed until a host adapter defines their ownership and version rules. Maintenance may
call ``collect_unreferenced(before=...)`` after an explicit retention grace period;
the Runtime does not schedule deletion automatically.

Session boundaries are explicit:

* ``StorageContext.session()`` is a **session lifetime** boundary only - no implicit
  ``BEGIN``/``COMMIT``/``ROLLBACK``; the caller owns commits (SQLAlchemy autobegin applies);
* ``StorageContext.transaction()`` is a transport-level transaction boundary
  (``BEGIN``/``COMMIT``/``ROLLBACK``) and contains no business unit-of-work rules. Epoch,
  lease and authority checks belong to the Runtime UoW (DS-P02-02), not to this adapter.

Installing only the base wheel leaves the migration CLI in place but unavailable: calling
``agent-runtime-migrate`` without the extra exits non-zero with a stable JSON
``MissingStorageExtra`` message instead of a traceback. An ``ImportError`` raised *inside*
an installed adapter keeps its real failure category instead of being reported as a
missing extra.

The state write path is ``agent_runtime.storage.SqlStateUnitOfWork`` (DS-P02-02). It is
constructed with the authenticated ``ActorContext`` and implements the frozen
``StateStore`` port: ``commit_advance``, ``commit_pause``, ``commit_terminal``, ``resume``,
``apply_input``, ``commit_cancel`` and ``commit_domain``, plus the non-port
``record_control`` intake and ``record_output_seal``. Every method opens exactly one
transaction, locks rows in the fixed order documented in ``storage/uow/locks.py``
(SessionSlot -> Run -> WorkItem/Command/Mailbox -> Budget/Capacity -> Domain Resource) and
re-reads the stored slot/run/work/lease/grant facts before writing; request DTOs supply
expectations only. Refusals are structured ``AgentRuntimeError(FailureEnvelope)`` values
classified as ``FENCED``, ``VERSION_CONFLICT``, ``CONTROL_PENDING``, ``FORBIDDEN``,
``INVALID_INPUT`` or ``CAPACITY_UNAVAILABLE``.

Flush evidence is re-enumerated from immutable ``output_seal`` rows, never inferred from
``event`` existence: each proof must match a stored cumulative channel digest, channel head
and durable watermark, and ``NoProducersConfirmed`` holds only when the run has neither a
seal nor an event delta. ``record_output_seal`` accepts a validated ``FlushProof``, verifies
the attempt/item/session/run ownership from stored rows, supports empty content or analysis
channels, and makes an exact replay a no-op while rejecting identity reuse with changed
payload. Authority is fenced **per run**: ``run.authority_revision`` must
equal the caller's ``auth_revision`` and the live capability grant revision, and grant
writers bump that fence while holding the same run row. There is no table-wide lock, so
unrelated runs never serialize against each other. Control intake (``record_control``)
writes the run's pending ``desired_control``/``control_command_id`` (and, for cancel, one
claimable ``reconcile_execution`` work item) so the safe point is reachable and a cancelling
run never dangles. Intake compares ``expected_version`` before writing and binds the complete
payload to the idempotency identity; pause/cancel are *applied* only through ``commit_pause``/
``commit_cancel`` with a full evidence bundle.

``SqlSystemCapacityStore`` implements reserve/release/observe over
``system_capacity``/``system_capacity_reservation``: the aggregate and reservation rows
change in one transaction, replays return the same reservation, capacity is never
oversold, and reclamation requires a holder confirmed stopped or terminal. A release
persists the canonical ``release_request_hash`` of its whole payload, so a replay is
``reused`` only for a byte-identical request and another payload under the same request id
is ``rejected`` (Q44).

``agent_runtime.storage.SqlJournal`` is the only Event writer. Output batches revalidate the
live Slot/Run/authority/Work lease and attempt/item ownership, lock the Session allocator,
enforce cross-transaction channel continuity, and commit Event plus Outbox atomically.
State-changing callers use ``append_state_in_transaction(session, batch)`` inside their own
UoW; ``append_state`` is only the standalone service-event boundary. Producer identity makes
exact retries reusable and rejects the same key with changed content without advancing the
session watermark.
``seal_output`` derives ``FlushProof`` from the committed Event text rather than trusting a
caller-provided digest: each channel digest is SHA-256 over its ordered UTF-8 text bytes,
and the immutable ``output_seal`` records both channel heads and the Session durable
watermark. An exact seal retry is reusable; after sealing, only historical batch replay is
accepted and new deltas are fenced.

``SqlJournalReader`` replays PostgreSQL as the source of truth. Public replay filters
restricted analysis while still advancing over its sequence numbers; authorized replay
revalidates the current session authorization revision. Gaps and authorization changes
return ``ResyncFrame``. ``SqlOutboxStore`` claims wake-up notices with
``FOR UPDATE SKIP LOCKED`` and a leased claim token. ``OutboxPublisher`` publishes through an
injected ``NotificationSink`` outside the transaction and acknowledges conditionally, so a
lost acknowledgement may duplicate a notice but cannot duplicate the Event. Scheduler claim
scans and orphan Run recovery remain DS-P02-04; no Redis or worker client is embedded here.

Validate wire events with `event_adapter.validate_json(...)` and terminal results
with `result_adapter.validate_json(...)`. The exported unions bind the event kind
or result status to a concrete payload. Snapshots and requests reject unknown
fields. These checks do not replace authorization or stream ordering validation.

Shared fixtures and generated JSON Schemas live in `agent_runtime/contracts/` and
ship in the wheel. After an intentional contract change, regenerate and review:

```bash
uv run python -m agent_runtime.contracts.schema src/agent_runtime/contracts/schemas
uv run python -m agent_runtime.contracts.schema src/agent_runtime/contracts/schemas --check
```

The distribution name and public release license require approval before public
publication. No package registry ownership is implied by this local project.

Internal delegation messages are validated through
`agent_runtime.contracts.messages.message_adapter`; next actions through
`agent_runtime.contracts.decisions.decision_adapter`. Transaction request shapes
live in `contracts.transactions`. Lease timestamps and epochs are observations,
not authority: an eventual database transaction must revalidate current ownership,
authorization, resource versions and deadlines. Handoff targets do not own the
source slot, and checkpoint maintenance uses a separate work lease.

`RunHandle` accepts an injected `RunBackend` and trusted actor/session/run identity.
It is a client, not an executor. Use `contextlib.aclosing(handle.events())` when
leaving a subscription early. `result(wait_timeout=...)` times out only the read;
it never cancels the run. Cancellation requires an explicit control request.
No persistent backend implementation is included yet; every backend call must
reauthorize the actor instead of trusting a previously created handle.

Normalized provider output uses `contracts.provider.provider_event_adapter`.
`ProviderIntegrity` validates one attempt. Limits are per single `ModelAttempt` and
independent per channel: `max_argument_bytes` covers all tool arguments of the attempt
(counted once, whether streamed as deltas or delivered only as a completion),
`max_content_bytes` bounds the public content channel and `max_analysis_bytes` bounds
the public analysis channel. Streamed arguments are compared with the completed
arguments through canonical JSON text, and additionally compares numeric source tokens,
so JSON type distinctions such as `true` vs `1` and `1` vs `1.0` are rejected even though
Python equality (and RFC 8785 number canonicalization) would treat them as equal; key
order is normalized. Feed the entire normalized stream before calling `completed_calls()`;
neither this validator nor a valid finish reason authorizes execution. Rejections name
a reason key in `provider_integrity.INTEGRITY_FAILURES`; DS-P01-03 promotes that table
into the reviewed error-code catalog. Provider-specific SDKs remain optional and tool
authorization is outside this layer. ``providers.openai_compatible`` implements direct HTTP
SSE normalization for online and private endpoints; ``SqlModelAttemptStore`` provides
idempotent begin and terminal CAS. The Provider layer still does not decide retry policy,
execute tools, settle the complete Run budget or authorize resources.

`ModelConfig`, `ProviderCapabilities` and `ProviderRequest` describe pinned model
configuration and snapshot-bound calls. `provider_preflight.validate_provider_request`
checks verified capabilities, deadlines and token limits. Nonempty provider options
require an injected trusted validator, including checks against the requested thinking
mode. Reserved option keys (transport endpoint, credentials, headers, HTTP client and
request-shape fields) are rejected in any spelling — case and `_`/`-`/`.` separators are
normalized, so `apiBase`, `api-base` and `API_BASE` all resolve to the reserved
`api_base` and can never override what the adapter assembles from trusted fields. This does not load context, authorize resources, resolve credentials or send HTTP.
The Agent stores only a model configuration reference, not a provider client or token.

Tool specs, invocation, approval bindings and outcome unions are in `contracts.tools`.
`tool_validation` checks identity, approval binding/expiry and declared public result
policy. Unknown or committed effects cannot request automatic retry. A waiting approval
or pending execution is not a terminal result. These checks do not create approvals,
query live grants, validate handler schemas, redact output, execute tools or enforce
database leases. Never treat a deserialized `ApprovedGrant` as proof of authorization.

`contracts/catalog.json` tracks service boundaries and core transactions, with explicit
missing pieces. Tests validate existing symbol and event references. It is coverage
metadata, not a service loader or evidence that every transaction is implemented.

`contracts.error_codes.ERROR_CODES` is the reviewed catalog of failure codes this
package emits; `FailureEnvelope` still accepts unknown producer codes, but tests scan
the source and shared fixtures so an unregistered code fails CI. Every content hash
(argument digest, policy input digest, journal output digest, state-event digest,
admission digest) uses `contracts.canonical.content_digest`, which implements RFC 8785
(JSON Canonicalization Scheme): alias-expanded input, object members sorted by UTF-16
code unit, compact separators, ECMAScript `JSON.stringify` string escaping and
ECMAScript `Number::toString` number formatting. The accepted number set is the
IEEE-754 double set: integers that are exactly representable are legal even above
`2**53-1` and print with ECMAScript's shortest text (`2**60` becomes
`1152921504606847000`), while values that would round (`2**53+1`, `10**30`, `2**400+1`),
overflow the double range (`10**400`) or are non-finite are rejected together with
non-string object keys and lone surrogates. Rejections raise `canonical.CanonicalJSONError`; every
public digest helper converts that into an `INVALID_INPUT` failure envelope.
Cross-language parity is verified rather than claimed:
`contracts/fixtures/canonical-vectors.json` holds 22 golden vectors, the independent
implementation in `scripts/verify_canonical_vectors.mjs` re-derives every text and
SHA-256 digest, and `tests/test_canonical.py` uses Node as the ECMAScript oracle for 2000
randomized doubles, the big-integer boundary and 500 Unicode strings (escaping vs
`JSON.stringify`). Any accepted digest input is byte-identical in Python and JavaScript.
Schema exports come from the single `contracts.schema.SCHEMA_REGISTRY`; a coverage test
fails when a concrete contract class is not reachable from any exported schema and when
two registry entries would silently shadow each other.

`contracts.base.Identifier` is printable text only: control (`Cc`), format (`Cf`) and
surrogate (`Cs`) code points, whitespace, NUL/CR/LF, bidi overrides and BOM are rejected,
and the exported JSON Schema pattern `^[^\s\p{Cc}\p{Cf}]+$` states the same rule for
other languages (apply it in Unicode mode). `contracts.base.MachineKey`
(`^[\x21-\x7e]+$`) is the narrower ASCII type for keys that are persisted and compared
across drivers: producer keys and idempotency/request keys. Free-form titles, summaries
and message text stay `Identifier` and are never narrowed mechanically.

Boundary validators raise one `AgentRuntimeError(FailureEnvelope)` for domain
rejections (`contracts.failure.runtime_failure`). Two layers keep native exceptions by
design: Pydantic model validators raise `ValueError` (reported as `ValidationError`),
and programming errors such as a naive `now` argument raise `ValueError`.

`contracts.scheduler` defines main-slot claim, renew, yield and input-wakeup shapes.
It does not schedule work. Waiting results require consumed input watermarks;
terminal wake results cannot contain a new work ID. Database locks, reauthorization,
response/request lease identity matching and child/maintenance ownership still require
implementation. Do not use the main-slot lease for all worker kinds.

`contracts.journal` separates bounded output drafts from committed receipts.
`journal_validation.validate_committed_batch` checks producer identities and content
digests. A `FlushProof` describes both output channel watermarks, but is not evidence
of durability unless issued and reverified against persistent sealed producer state.
The ``storage`` extra implements journal, outbox and flush operations; a forged
``FlushProof`` still cannot replace storage-side verification.

`contracts.transitions` models main-run pause and terminal commits. Pending control
blocks successful completion; cancellation requires stopped executions, not merely
isolation. Unknown executions on failure require a supervision reference. Every proof,
execution set, command and observed state must be rechecked in the eventual transaction.
Terminal commits carry a `FlushEvidence` union instead of a possibly-empty list: either
enumerated `FlushProof`s (`verified`) or an explicit `no_producers` confirmation, both
bound to the committing run and to a positive session watermark that cannot predate the
evidence they report. The union prevents *omitting* evidence; it does not prevent
*forging* it, so T9 must still re-enumerate the durable producers.

`contracts.context` describes preparation inputs and pinned snapshot metadata.
``ContextRuntime`` loads authorized, versioned sources and stores an immutable budgeted
payload, including the trusted Prompt text and current user input. ``ContextBoundProvider``
checks snapshot, digest and current access before the model stream opens; the bundled
``ContextOpenAIInputResolver`` builds provider messages and tool definitions only from
that snapshot. The ``storage`` extra currently provides PostgreSQL context Blob,
snapshot and owner-only goal/plan readers. Attachment and knowledge readers require a
host ACL adapter. ``PolicyRuntime`` constructs validated allow/deny/approval decisions
from a host authority; only that authority can issue a grant or persist an approval.
``RunActionDispatcher`` demands a persisted decision before asking Policy and executing
a tool. Tool execution must revalidate the grant, budget, actor and resource versions
under its own transaction. Model output is never a grant.

``SqlWaitActionCommitter`` commits staged Wait and AskUser actions through the fenced
Scheduler. AskUser requires a registered ``message_predicate`` and a trusted
``message_deadline_at`` no later than the Run deadline. Its ``input.required`` event
contains the question, required facts, recipient Run and state version; it commits with
the waiting state and Outbox. A new pending input requeues the Run without publishing
an obsolete question. ``SqlWaitWaker`` requires a matching persisted wait and pending
input before scheduling further work.

``SqlAnswerActionCommitter`` handles staged Respond/Finish decisions. A trusted
``AnswerPublisher`` resolves the answer request and must reuse output by Run/decision
Attempt on retries. It returns Item IDs only after model output is journaled and sealed;
the SQL adapter checks those facts again. Respond atomically completes the Items and
queues a unique continuation. Finish requires published, sealed, nonempty final answers,
no pending user input/control, no active model/tool calls and confirmed stopped executions.
It commits Run/primary Turn outcome, WorkItem/Slot release and Journal/Outbox together.
Evidence references require injected host resource ACL/version checks. Output failures do
not become empty successful responses; the staged decision remains recoverable.

This increment supplies SQL commits and the publisher port, not a complete answer-model
assembly or Web application. Live Provider/Worker integration remains pending.
No public event schema or database migration is added by this increment.

`contracts.ownership` separates child leases/workspace proposals, maintenance execution
observations and recovery dispositions. A child lease has no main slot version, and a
supervisor observation is not a Run finalization request. Actual root cancellation,
workspace ownership, live leases and external process state must be rechecked by the
future persistent runtime. These models do not start, stop or recover tasks.

`ports.py` publishes the module Protocols; `provider.py` defines asynchronous streaming
and cancellation ports. Capability
manifests for Hook, Prompt, Knowledge, Skill and remote Agent live in `contracts.registry`.
An Agent can pin input/output schemas; ordinary answers need not select structured output.

`contracts.credentials` never serializes long-lived credentials or arbitrary destinations.
`credential_validation.validate_proxy_request` resolves the egress target from the trusted
`EgressRegistry` by reference (a caller never supplies host/port/path), validates the DNS
answer set through `approved_addresses`, and re-checks scope, actor, grant, generation,
allowance, revocation and payload digest before returning the approved numeric addresses
to connect to. `validate_filtered_response` checks response identity, byte allowance and
the delivery deadline. The adapter must keep TLS hostname verification, refuse redirects,
and connect to one of the returned addresses.
Workspace manifests reject unsafe paths and restoration requires a new generation;
the actual filesystem adapter must still reject symlinks and verify all bytes.
Mailbox claims/applications have separate watermarks, cancellation acknowledgements are
not stopped-process evidence, and child collection requires versioned acceptance evidence.

Repository-only dependency probes live in `scripts/verify_runtime_dependencies.py` and
`scripts/runtime_fencing_probe.py`. Their isolated Alembic fixture is a concurrency spike,
not the distributable production schema. Full P01 gate status is recorded in
`docs/agent-runtime-p01-acceptance.md`; passing package tests alone does not close that gate.

The live P03 probe is ``scripts/verify_runtime_agent_loop.py``. It exercises a real
OpenAI-compatible model with answer, tool/observation/follow-up, clarification and
cancellation paths, but uses in-memory probe stores and is **not** a recovery or
production SQL authorization test. It writes only aggregate metrics under ``output/``.

`storage.SqlPlanActionCommitter` commits staged UpdatePlan decisions through the fenced
SQL UoW. New plans require expected revision 0; updates append immutable revisions using
CAS. Plan pointers, selected checkpoints, Journal/Outbox and persistent continuation work
commit atomically. The standard source reader performs evidence ACL checks on the same
transaction connection. This adapter does not itself assemble a Provider/Worker/Web app.

`storage.SqlDelegateActionCommitter` binds staged Delegate decisions to
`storage.SqlSubagentRuntime`. Inject trusted `DelegationPolicy` entries, transactional
resource access and an `AcceptanceVerifier`. Spawn atomically reserves a child budget,
creates its Task/Goal/Run/Workspace/grant, persists the assignment and queues durable work.
The scheduler claims the assigned child allocation, never the root allocation. Child
context reads and snapshot checks enforce the pinned resource/tool grant; final tool
execution must independently enforce it too.

`propose` fences child leases, root cancellation and workspace ownership, requires stopped
executions and sealed output, then persists a candidate and wakes only an eligible parent.
`collect` verifies parent/child identity and ordered input, independently validates the
candidate, records acceptance/rejection and queues the parent continuation atomically.
Known usage settles actual consumption; unknown usage retains the reservation. Finish
refuses active children and unverified proposals. Verification receipts addressed to a
completed child are durable pending messages, not instructions to restart that child.

This SQL increment supports one delegation level. The native child worker now assembles
ContextRuntime, ProviderInvoker/ModelAttemptStore, DecisionStore/LiveRunGuard, separated
output events and immutable seals, budget audits, and read-only local tool observations.
It publishes a sealed TimelineItem as a candidate result. The standalone host assembles
terminal notification consumption and terminal-child budget reconciliation. Host-specific
Artifact readers remain an explicit integration responsibility.

`WorkerRunner(child_handler=...)` supplies a refreshed `ChildLeaseHandle` to child work.
Inject `SqlSubagentRuntime.handle_worker_failure` as `child_error_sink` to report a
handler failure with the current child lease. Fenced writes and unresolved executions
are refused rather than silently converted into successful cleanup.
Lease renewal failure cancels local execution; draining the worker is not user cancellation.
`SqlSubagentRuntime.report_failure` atomically records terminal failure/cancellation,
notifies the parent and wakes it only when eligible. It refuses unresolved calls/processes
and requires persisted control for cancellation. Child control commands and residual
advance/control work converge in the same transaction.

Delegation time is the intersection of the requested deadline, parent deadline and the
pinned target Agent's runtime budget at spawn. This absolute deadline is persisted once
in the child Task, Run, assignment and Work; renewal and recovery do not extend it.
`DelegationPolicy` allocates call/token budgets; unknown audited usage stays reserved until
trusted reconciliation, rather than being refunded as zero. Cooperative pause waits for
already-owned child work to reach its bounded safe point, while blocking new child claims.

External completion waits use `WaitSpec(kind="external", ...)` and a host-installed
`SqlWaitWaker.external_predicates` callback keyed by a pinned `CapabilityRef`. The callback
checks durable completion evidence and current access inside the wake transaction; an
HTTP notification is not itself completion. Missing callbacks, stale versions, pending
control, expiry or revoked access never authorize advancement. The standalone `external_wait` profile
demonstrates immutable, authenticated result insertion and restart-safe maintenance scans.

`reconcile_budget(child_ref)` can settle an owned terminal child's already-audited usage
after collection. It returns false while usage/process state is unresolved; concurrent or
repeated calls reuse the settlement identity. This is not a Provider usage ingestion service
or a periodic reconciliation worker. Main-slot adapters must not use fabricated child
authority.

Child SQL adapters now accept an explicit `ChildCommitGuard` getter together with the
current `ChildLeaseToken` in `SqlDecisionStore`, `SqlLiveRunGuard`, and `SqlModelEventSink`.
The getter must follow the worker's renewed lease and confirmed state; it does not silently
upgrade cancellation, authority, or workspace versions. The shared child fence locks the
Slot only for ordering and never grants main-slot ownership.

`ChildProducerBatch` and `ChildSealOutputRequest` are in-process storage requests, not new
external wire contracts. `SqlJournal` persists child content/analysis with parent identity,
channel continuity, restricted analysis visibility, deduplication and immutable seals.

Inject `SqlChildToolAuthorizer` into the executor's effect/ledger transaction and call
`require_in_transaction` before database effects. It checks current child authority,
tool/resource grants, the persisted call/arguments, completed model attempt and exact
snapshot/version, current staged checkpoint, and live resource ACLs. The executor must
derive all resource targets from trusted tool schemas/effects, retain Policy/HITL and
idempotency checks, and reconcile external effects separately. This authorizer is not a
complete ToolHost and cannot roll back a network call.

`SqlChildWorkerHandler` is a `WorkerRunner.child_handler` for native `Respond` and
`InvokeTools`. Inject pinned `ChildAgentBinding` entries, a long-lived Provider backend,
capability resolver, token estimator, trusted tool specs/handlers, policy, and the same
`SqlSubagentRuntime` used for `child_error_sink`. Local/SQL reads remain bounded to
`side_effect=none`, `read_only` idempotency and full results. An optional `SqlToolHost`
allows exact registered transactional database writes and provider-key external tools.
Its injected transaction authorizer must call `SqlChildToolAuthorizer` with the current
renewed guard; the host must supply real schema validators, a side-effect-free policy and
trusted target resolvers. Database writes and `operation` commit share one transaction.
External dispatch records intent before sending, uses the operation ID as the remote
idempotency key, and creates an `execution` plus `reconcile_execution` work in the same
transaction. It only calls `lookup` on re-entry while the ToolCall is still pending;
an unresolved effect stays `unknown`; the child parks in `waiting_tool` and releases its
work lease without repeating the write. `SqlExecutionSupervisor` can query/cancel with a
maintenance lease. For a parked child, install its `subagents` adapter: after remote
confirmation it atomically fails the child, publishes a `task.failed` notice to the parent,
and closes the supervision work. Supervision work extends its claim deadline instead of
silently expiring at the original tool deadline. The supervisor does not synthesize a lost
Agent Observation or reopen an unknown Operation. The host must install a trusted remote
inspector and schedule maintenance workers. This path does
not yet wire credential brokerage into the host, general MCP registration, interactive
approval, or user-facing reconciliation after a terminal unknown ToolCall.
A child cannot wait for interactive approval: ask/review becomes a denied observation.
Unknown model usage blocks further child model calls; completed but uncommitted attempts
are not replayed. The isolated SQL/real-provider Worker test exercises the SDK assembly;
Product-host startup remains separate integration work. A disposable process-level gate
(`scripts/verify_runtime_worker_processes.py`) exercises two real `WorkerRunner.serve()`
processes, SIGKILL, SQL recovery, epoch fencing and zero schema leftovers. The
OpenSandbox probe (`uv run --script scripts/verify_runtime_opensandbox.py --reliability`)
starts a pinned disposable server and verifies background process cancellation, resource
limits, deny-by-default egress, restart and TTL cleanup. For an opt-in local MCP effect
fixture, run `uvicorn scripts.runtime_test_mcp:app --host 127.0.0.1 --port 18151` with
`CENTN_TEST_MCP_DB` and `CENTN_TEST_MCP_TOKEN`, then
`python scripts/verify_runtime_mcp_effect.py`. The fixture is loopback-only, persists
idempotency keys in SQLite, and is not a production credential or MCP gateway.

### Root Worker host integration

Install the repository host dependency with `uv sync --extra runtime-host`. The reusable
`SqlClaimActorResolver` reads each root claim's owner and live grant from SQL under the
Slot/Run/Authority/Work fence. `SqlClaimMainHandler` opens a user-bound handler per claim;
do not share one fixed-Actor handler between users. `RuntimeUserHandlerFactory` in
`app/services/runtime_worker_service.py` also reloads the host account and principal,
rejecting disabled/deleted users and tenant changes without anonymous/default-tenant
fallback. Its injected factory owns Agent bindings, ToolHost, ACL and output ports.

`RuntimeWorkerService` assembles main/control lanes with `SqlWorkerRecoverySink` and
`SqlWorkerService`, which supervises periodic SQL recovery alongside Runner and drains
both on shutdown or failure. The embedding process owns HTTP clients and the SQL engine;
it must await `serve()` and call `request_drain()` on shutdown. This service is an explicit
host assembly, not an automatic replacement of the current Web/Celery routes.

`SqlMainWorkerHandler` defaults to `SqlMainControlHandler` for verified safe-point
pause/cancel. Running external executions, active models and unsealed producers prevent
terminal cancellation; they require supervision/output recovery, never fabricated stop
evidence. A host can inject a broader control handler using the same state UoW.
Private Qwen tests use a host-owned `httpx.AsyncClient(trust_env=False)`; do not inherit
system proxies accidentally for LAN endpoints or disable external-provider proxies
globally inside the adapter.

### Installed profiles and Journal transport

Root claim recovery seals abandoned streaming items before starting another model attempt.
It preserves the producer referenced by the current staged Decision, so a recovered answer
or tool decision can still commit. Interrupted output remains cancelled partial output,
not a completed answer. A crash between sealing and publication can be recovered again;
this does not reconnect an interrupted Provider stream or recover uncommitted tokens.

`SqlAgentInstaller.install(scope_id, ref, agent)` persists immutable SDK Agent revisions.
Concurrent identical installations reuse the same revision; changed content is rejected.
Installing a historical revision never downgrades the published pointer. Agent storage
currently has a non-namespaced key: a different namespace owning the same key is rejected.
Installation is trusted operator/host work, not an authorization grant or a public endpoint.

`SqlMainHandlerFactory` accepts pinned `MainAgentProfile` objects, shared HTTP/SQL clients,
and an explicit `MainPortsFactory` for policy, tools, resource ACL and subagents. It creates
current-Actor Provider/Context/Loop instances per claim. `SqlMainLoopSetup` reads actual
persisted inputs and bounded historical resources; it does not use fixed test questions.
Unimplemented Skill/knowledge/hook/Agent-policy/custom-context bindings are rejected, not ignored.
The repository host bridge is `RuntimeAgentInstallationService`; installation and claim
authorization are separate checks.

Post-answer control decisions bind a pinned output schema to the Provider response
format; prompt-only JSON instructions are insufficient. The host must verify structured
output support. Final item and evidence candidates are constrained to sealed outputs and
version-pinned included snapshot sources, and SQL commit validation remains authoritative.
Native answer streaming does not use this control response format. Output-format callbacks
retain their process-owned clients; static format mappings are defensively copied.

The independent process entry is:

```bash
RUNTIME_WORKER_FACTORY=your_host.bootstrap:worker bash scripts/runtime-worker.sh
```

The factory is a trusted async context manager yielding `RuntimeWorkerService`; it owns
clients, installed profiles, current account/principal lookup and cleanup. SIGINT/SIGTERM
request drain before resource cleanup. This entry has no default production bootstrap
yet: the command must not be described as a ready-to-run deployment or API cutover.

`RuntimeJournalService` emits `journal.page` SSE frames, whose `id` is the page's decimal
cursor. Pages advance over restricted events without exposing them. Reads share one MVCC
snapshot; sockets do not own tasks. `web/src/runtime/runtimeJournal.ts` provides an atomic
public page reducer with isolated content/analysis channels. The existing API and Chat
page have not yet been switched to this transport; snapshot resync and host route wiring
remain required before production cutover.

### Bounded model and context recovery

`ProviderInvoker` records truncated, empty and unterminated output as a failed attempt
and propagates that exact failure to the loop. Complete tool arguments are withheld
until a successful terminal event. Recovery never joins abandoned output to a new attempt
or silently increases the Agent's output allowance. Repeated failures exhaust the bounded
retry policy; malformed streams and content filtering are not transparent retries.

`SqlRecovery` rechecks current claim, authorization, usage, budget and unresolved effects
before scheduling another model attempt. Unknown usage is not zero and blocks automatic
budget reuse. OpenAI-compatible context rejections use explicit profile error codes only;
unreported usage on an HTTP rejection stays unknown, even when inference probably did not run.

`SqlMainLoopSetup` halves the previous input bound after a persisted context rejection and
retains that reduced bound through subsequent decisions. Root context preparation may omit
automatically loaded history, prioritizing recent sources; current input, tool observations
and explicitly referenced historical resources remain mandatory. Omission is recorded in
the immutable snapshot. Mandatory facts that do not fit cause an explicit failure, not text
truncation. This is budget pruning, not semantic summarization or long-term memory.

Before calling the model, a context resource conflict may resolve trusted Setup once more,
only if its source references actually changed without altering actor, model, tools or
budget boundaries. Authorization, state/checkpoint CAS and tool-effect conflicts are not
handled by replaying old decisions. Child-specific automatic retries require their
separately specified integration.

### Terminal outcomes

Root Recovery commits exhausted/deadline failures with captured output, immutable
checkpoint, Turn outcome pointer, terminal Journal events and Outbox in one transaction.
Previously completed Timeline items remain unchanged. Interrupted public text is retained
as failed, never mixed with the private analysis channel or promoted to a successful answer.
Published artifact references remain available; unresolved external effects keep their
independent supervision work and must be checked before repeating an operation.

New root terminal events include `publicMessage` and `nextSteps`; old events without
these fields remain readable. Suggestions do not grant permission or authorize retries.
Partial completion still requires verified sealed final output, and cancellation still
requires actual stopped-execution/flush evidence. Root cancellation retains earlier
completed output and published artifacts as well as interrupted text.

### Live recovery verification

Opt in with `AGENT_RUNTIME_LIVE_PROVIDER=1`, `BASE_URL`, `API_KEY`, `MODEL_NAME` and
`AGENT_RUNTIME_TEST_DSN`. Run the `test_main_worker_commits_live_qwen_answer`,
`test_main_worker_live_qwen_fault_recovery` and
`test_main_worker_interrupts_live_qwen_and_recovers_output` tests in
`tests/test_storage_uow.py`. Database fixtures require the dedicated test database
`aletai_wangxh`, create randomized schemas and verify their cleanup.

The matrix uses actual Qwen inference and SQL Worker commits. Length/empty-output faults
modify normalized Provider events while retaining genuine usage. Timeout/disconnect faults
interrupt HTTP bytes after the first genuine content SSE frame, before usage is delivered.
These are deliberate fault injections, not evidence of a spontaneous upstream outage.
Unknown usage must stop automatic retries. The separate SQL effect-safety matrix validates
refusal to replay unresolved writes and retention of supervision, not a production ToolHost
or sandbox certification. Never use a production database for these tests.

### OpenSandbox execution adapter

Install the optional integration with `uv sync --extra sandbox` from this package.
The bridge pins `opensandbox==0.1.16`; core imports do not require the SDK.
`SandboxExecutionRuntime` takes an explicit backend, authority and clock. A trusted host
connects an existing sandbox with `sandbox.opensandbox.connect_opensandbox`; the model
must not choose sandbox credentials, image bindings or host mounts.

Starts require a durable authority reservation before remote dispatch. A lost response
or failed handle write must retain an unresolved intent, not replay the command. The
SQL dispatch persistence is available through `storage.sandbox_start.SqlSandboxStartAuthority`.
It requires explicit live transaction and execution authorizers: the host must verify
lease/ACL, actual image/workspace identity, command derivation, capacity and budget in
that transaction. There is no default-allow authorizer. Requests and immutable bindings
are kept in the shared Blob store; the database stores their reference and remote handle.
Status requires an explicit exit code to report success; interrupt acknowledgement is
not stopped-process evidence. Closing the client does not delete the sandbox. Cleanup
requires confirmed deletion. Background logs are combined, not separate stdout/stderr;
the returned-content bound is not a server-side disk or network-memory quota.

From the repository root, run the opt-in Docker integration probe:

```bash
PYTHONPATH=packages/agent-runtime/src uv run --script scripts/verify_runtime_opensandbox.py --runtime-adapter --output output/p04-01/opensandbox.json
```

This probe uses dedicated sandboxes and a volatile test authority. It verifies the bridge,
not SQL authorization, durable takeover, production port isolation or complete ToolHost
integration. Those separate gates passed in E04 below; this bridge-only command is not
a substitute for them or for production-security validation.

`SqlSandboxMaintenanceWorker` plugs into `WorkerRunner` as a maintenance handler and
revalidates its lease before inspection and before committing evidence. It persists
terminal process evidence separately from cleanup, and requeues unconfirmed cleanup.
Supply a trusted connector and a deletion verifier (for OpenSandbox,
`confirm_opensandbox_deleted`). Only a dedicated, non-reassigned sandbox may be deleted.
A committed result must exist before cleanup: process completion alone must not destroy
logs or output awaiting collection. Deletion-response loss can be reconciled using typed
absence evidence, never by interpreting a connection failure as success.

`SqlExecutionMaintenanceRouter` selects this handler or the existing external-effect
inspector using the persisted work payload, not model-provided routing data. It can be
passed as the maintenance handler of an existing `WorkerRunner`; no new process-local
task registry is introduced.

Register a `BackgroundExecutionTool` in `SqlToolHost(background_tools=...)` for external
`reconcile` capabilities. The handler must use the SQL start authority and return a
generation-pinned `PendingExecution`. Observation persistence maps it to `ToolCall.running`
without a terminal event. Configure `execution_predicate` on main/child handlers (and list
it in main `allowed_predicates`); the Loop then releases its lease while awaiting results.
Without explicit configuration the Loop still fails closed instead of replaying a call.

Every SQL ToolHost requires an injected `SqlToolCapacity` backed by the shared
`SqlSystemCapacityStore`. Declare each scope/concurrency-group capacity at deployment
bootstrap, with the pinned `max_concurrency`; all Workers in that deployment must use
the same deployment ID and database. A process-local semaphore only reduces local
contention. Inject the same capacity adapter into `SqlSandboxResultCollector` so a
trusted terminal result releases its slot in the ToolResult transaction. A missing
pool fails closed; a pending or unknown external effect keeps its slot until verified
terminal evidence. `SqlExecutionSupervisor` resolves the ToolSpec from the persisted
ToolCall and immutable capability revision, then releases the slot in its fenced
terminal transaction. Revoking a capability does not prevent cleanup of an earlier
effect. Main and child SQL Workers require a `SqlToolRuntimeAssembly` for effectful
ToolHost bindings; it checks that the host and required terminal handlers use the
same engine, schema and capacity adapter. Separate processes construct their own
adapters with the same deployment ID and database; deployment must actually run the
maintenance lane, not merely construct its handler.

SQL ToolHost also requires `SqlToolExecutionGate`, configured with a trusted
`approval_required(invocation, spec, resources)` policy resolver. It locks the current Run and budget
allocation in the execution transaction; an approval-required tool additionally
requires an approved, unexpired row bound to the exact operation and a grant ref
`approval:<approval-id>:<revision>`. Its approval request key is
`tool:<operation-id>`. The gate verifies an existing approval; the host must still
install a durable request/decision/wakeup service and enforce policy when issuing
the grant. `SqlApprovalService` commits decisions and `approval.updated` Journal/Outbox
facts atomically. `SqlApprovalWakeScanner` also closes pending approvals when Run,
Session or grant authority ends. Inject its `resource_validity` transaction port to
recheck application-owned source ACLs and versions: the SDK cannot infer private
resource revocation without that port. Definitive denial cancels; unknown dependency
errors roll back, and valid rows rotate through a bounded durable scan. This validity
check is separate from authorization for a human to approve. The standalone host
installs the port and renders all decisions from the Journal, not a second UI poll.
The disposable two-maintenance-process gate is
`scripts/verify_runtime_p04_tool_host.py --process-only` against `aletai_wangxh`;
it does not install the product's `app.runtime_worker` bootstrap or liveness monitor.

The R02 reference host is `scripts/verify_runtime_p04_host_worker.py` and its gate is
`scripts/verify_runtime_p04_tool_host.py --host-process-only`. It starts separate
main, child and maintenance `app.runtime_worker` processes against isolated SQL,
loads pinned installed Agents and model profiles, and binds ToolHost, approval,
ACL, capacity and maintenance ports. `SqlMainHandlerFactory` and
`SqlChildHandlerFactory` receive the current claim lease; the root ToolHost
authorization transaction fences stale claims. This is SDK host acceptance, not a
switch of the existing application's routes or its process supervisor.

Inject `SqlSandboxResultCollector` into the maintenance worker via `results`. Its required
reader must bound logs/files and sanitize output; its transaction authorizer must verify
current ACL/workspace generations without needing the relinquished main lease. Output
schema validation and public projection precede the atomic ToolCall/Operation/Journal
commit. Private collection evidence is sealed separately. A committed operation records
that execution occurred, including nonzero exits; it does not imply business success.

The persistent maintenance work retries result notification even if completion/cleanup
preceded the owner's wait. Wake preserves the staged decision, waits for all its calls,
checks maintenance fencing and respects parent/owner pause and cancellation. Cleanup
and notification are separate obligations. Quotas and the real OpenSandbox+SQL+Worker
fault matrix passed the E03/E04 gates below, independently of fake-remote SQL tests.

### Resource supervision and bounded OpenSandbox pool (E03)

Trusted hosts may bind `sandbox.supervision.ExecutionLimits` to `ExecutionStart.limits`.
The limits become part of the immutable dispatch payload/hash. Configure maintenance
`inspect_resources` to return execution-bound `ExecutionUsage` from a trusted supervisor.
Counters are cumulative nonnegative integers; missing/invalid measurements fail closed.
Do not measure untrusted files by executing model-supplied inspection commands.

Stop reasons are committed under maintenance fencing before interrupt, and survive Worker
replacement. Deadline/cancel/quota stops never become success on exit code zero. For
limited executions, command exit alone is insufficient: the inspector must confirm the
whole process tree stopped, otherwise status stays unknown and maintenance is retried.
Failure details are sealed privately and public results include a reason and next step.

The fixed Docker pool now enforces limits independently of maintenance polling:
read-only root, kernel tmpfs byte/inode ceilings for `/workspace` and `/artifacts`,
and a trusted root guard retaining at most `log_bytes` of combined user output.
User commands run as uid/gid 1000 with no supplementary groups or control credentials.
An inherited Linux syscall filter denies socket/socketpair/io_uring creation, including
access to the local execution-control API. This pool is deliberately offline.
Cancellation/deadline stops use pidfds and verify all user processes are gone, including
detached `setsid` descendants. Measurements bind execution/command identity and limits;
they come from kernel allocation/inode state, not user-generated reports.

Launch a dedicated server with an operator-owned TOML config, authenticated API and
JSON limits file (page-aligned bytes; logs at most 1 MiB):

```bash
uv run --script scripts/run_runtime_opensandbox.py --config /path/server.toml --limits /path/limits.json
```

Set `OPENSANDBOX_SERVER_API_KEY` before launch. Example limits file:

```json
{"log_bytes":65536,"workspace_bytes":1048576,"artifact_bytes":1048576,"artifact_count":8}
```

This explicit server extension pins OpenSandbox server 0.2.3 / SDK 0.1.16; pin workload
and control image digests in operator config as well. Unsupported server versions fail
startup. Linux x86_64/aarch64 are supported by the guard; the recorded Docker acceptance
only proves the actual test host architecture. Bind maintenance `inspect_resources` to
`agent_runtime.sandbox.opensandbox.inspect_opensandbox_resources`. A trusted result reader
can call `OpenSandboxBackend.read_output()` before sealing and cleanup; raw bytes are
private until the existing ToolHost public projection is applied.

Artifact count limits inode entries (including directories/hard links), not semantic
report count. Deleted-but-open allocations remain counted by the kernel. `/tmp` (16 MiB)
and shared memory (1 MiB) are separately bounded scratch space, not artifact storage.
The guard's private output is bounded; operator/server log retention is a separate concern.
Tmpfs is ephemeral: no claim of cross-host workspace or Python-memory recovery.
Existing requests without limits retain prior adapter behavior, not hard-quota guarantees.

Real acceptance: `verify_runtime_opensandbox.py --hard-quotas --runtime-adapter` passed
30 checks with no residual probe containers. The additional E04 integration gate below
has passed; neither gate is a complete production-security sign-off.

### SQL/OpenSandbox/independent Worker acceptance (E04)

```bash
PYTHONPATH=packages/agent-runtime/src uv run --script scripts/verify_runtime_opensandbox.py \
  --hard-quotas --runtime-adapter --sql-worker --output output/p04-01/e04-combo.json
```

Requires the package development environment (`uv sync --project packages/agent-runtime`),
Docker, and PostgreSQL `aletai_wangxh`; DSN is read from `AGENT_RUNTIME_TEST_DSN` or the
root `.env`. The probe rejects other databases before creating a random schema. It uses
test fixture authorization, not a production identity/ACL adapter. Workers are separate
processes using WorkerRunner, SqlScheduler, SQL result sealing and the real SDK bridge.
Cases cover success/wake, nonzero exit, quota, deadline, cancel, killed Worker/takeover,
sealing rollback, lost deletion response and lost dispatch response (unknown, never replay).
Credentials use child-process environment, not command arguments. Only probe-owned
sandboxes, Workers and random schemas are cleaned. Evidence stays in ignored `output/`.
Unknown dispatch remains unresolved in SQL; fixture teardown is not runtime success or
an automatic compensation. This gate does not migrate existing application routes.

Accepted on 2026-10-13: all nine SQL scenarios and 40 combined checks passed; all nine
random schemas were dropped, SDK cleanup succeeded and no probe containers remained.
Evidence: `output/p04-01/e04-combo-sealed.json` (local ignored artifact), with the durable
review in `docs/agent-runtime-p04-code-review.md` section 6. Cancellation verifies stopped
execution, sealed output and a claimable control task, not final main-Run cancellation.
Credential mediation is described below; this E04 probe does not exercise it.

### Credential mediation (DS-P04-02, SDK accepted)

Install the optional `broker` extra for `credentials.https.PinnedHttpsTransport`.
`CredentialBroker` requires explicit `GrantStore`, `CredentialVault`, `BrokerBlobs`,
`EgressRegistry`, `AddressResolver`, `EgressTransport` and `ResponseFilter` ports.
Construct it once with host-owned dependencies; close the transport at lifespan shutdown.
The caller argument must come from authenticated host transport, never from a model's
request. A serialized GrantRef is not a bearer credential.

Only a fixed destination revision is accepted. The HTTPS adapter validates every DNS
answer, dials a pinned numeric IP with original-host TLS verification, refuses redirects
and compression, bounds response bytes and enforces a total timeout. No environment
proxy, automatic retry or keepalive reuse is enabled. Secrets are never persisted by the
Broker; raw responses are inspected before private Blob storage. Secret-echo checks are
not comprehensive DLP: the host must provide its own trusted response filter.

GrantStore must durably bind each requestId to its complete request, atomically reserve
allowance, revalidate current authority on sending/delivery/replay and never replay an
unknown request or refund it on process death. Only sealed results may be reused.
B01 tests use an explicit memory fixture, **not a production store**.
`storage.credential_broker.SqlCredentialGrantStore` is the PostgreSQL adapter (install
`storage` and apply the Runtime migration chain). Its mandatory transactional `authorize`
callback verifies current host Policy, resource ACL and destination/input binding; it must
not perform network I/O or acquire locks preceding SessionSlot/Run. ActorContext must come
from authenticated host ingress. The store rechecks live slot/root/child ownership,
authority revision, execution generation/deadline and operation state. Grant-locked durable
request rows enforce per-grant allowance. Mutation and private audit commit together;
reserved/unknown requests never replay or refund automatically. Revocation blocks cached
delivery as well as new dispatch. The host must authorize any newly scoped grant and
enforce aggregate task/account quotas separately; changing expiry is not free renewal.
Audit immutability is an adapter contract, not a claim of tamper resistance against a
database administrator. Ambiguous reservation cancellation remains durable `reserved`;
failed duplicate reservation must not settle the original caller's in-flight request.

`CredentialIngress` and `CredentialServiceClient` use TLS 1.3 mutual certificate
verification, not a public browser endpoint. Provision CA/server/client certificates and
private keys through host deployment. The trusted control plane calls
`store.bind_service(actor, grant, sha256_of_client_certificate_der)` once. The SQL store
implements `ServiceIdentityResolver`: only a matching authenticated certificate resolves
the stored actor for that exact grant. Rotation requires new authorization; there is no
remote bind/issue/admin endpoint. Grant revocation blocks service access and replay.

The single-request TLS RPC uses a 4-byte network-order length followed by UTF-8 JSON
(`ProxyRequest` in, `FilteredResponse` or sanitized error out). It bounds frame sizes,
authenticated request concurrency, handshake and total time; clients do not auto-retry.
Deployment must additionally rate-limit TCP/TLS handshakes at the service perimeter.
Caller JSON/headers cannot establish identity; a grant reference alone grants no access.
The service distinguishes denied requests, unavailable identity service and uncertain
proxy outcomes. The client maps these to `FORBIDDEN`, `DEPENDENCY_UNAVAILABLE` and
`SIDE_EFFECT_UNKNOWN`; connection loss is conservatively uncertain. The Broker sanitizes
both proxy and control-plane dependency failures. Never use the public error to infer that
an external service did not execute a request. Inspect the SQL ledger before reconciliation.

`sandbox.credential_relay.SandboxCredentialRelay` holds the service client in the host.
Workloads write `/workspace/credential-request.json`; the guard verifies execution,
file type/owner and byte limits with no symlink following. Only the host-authorized grant
is forwarded. The filtered result reference is atomically published create-only to
`/workspace/credential-response-<requestId>.json`. Private body retrieval still needs an
authorized Runtime Blob tool; raw upstream bytes are not implicitly copied to the sandbox.
User processes stay network-denied and receive no host credential mount or service key.
Do not bypass the guarded start path when using this relay. Dependency ownership,
server/client shutdown and certificate renewal are explicit host responsibilities.

B04 accepted: `scripts/verify_runtime_credential_ingress.py --faults` runs a real
SQL/mTLS/HTTPS/OpenSandbox combination with independent Broker processes and nine fault
scenarios. It passed 22 required checks with no extra dispatch for reserved/unknown
requests; package regression passed 1213 tests (275 skipped). See
`docs/agent-runtime-p04-code-review.md` section 10 and local ignored evidence under
`output/p04-02/`. SQL failure is simulated by connection refusal in the test Broker;
Vault/filter/Blob policies are fixtures, not deployed production services. Host PKI,
production Vault/Blob/DLP, Broker deployment and Worker integration remain host work.
Unknown external effects require reconciliation; this SDK does not promise exactly-once
execution at an external service or switch existing application routes.

### Capability registry (DS-P04-03 R01)

`storage.capability_registry.SqlCapabilityRegistry` installs immutable
`CapabilityManifest` revisions in the existing SQL capability tables. Supply a trusted
`CapabilityAuthority` for configuration changes and every Tool use; model arguments never
identify the caller. `publish` serializes on the capability row, moves the old active
revision to `superseded`, and exposes only the new active revision to discovery. A Run may
resolve a pinned superseded revision through `PinnedToolSpecResolver` after current host
authorization; `revoked` is always denied. The tool catalog hash is canonical and rejects
conflicting limits within one concurrency group. Apply migration
`0007_capability_superseded` before using superseded revisions; a downgrade refuses to
discard this state while such rows exist. No Tool handlers or remote connections are stored
in manifests. See `docs/agent-runtime-p04-03-plan.md` for R02–R06 integration work.

### Installed Skills (DS-P04-03 R03)

`SkillSpec` pins a Git commit, optional `gitSubdir`, file hashes/modes and bundle digest.
`PublicGitFetcher` requires an allowlisted HTTPS repository and a host-controlled HTTP
proxy; fetching never executes repository scripts. `GitSkillInstaller` validates limits
before reading files and rejects symlinks, escaped paths and byte/permission drift.
Private deployments may supply a host-owned `ca_bundle`; TLS verification stays enabled.
The proxy must enforce destination/DNS/IP policy. Neither model arguments nor repository
configuration may override that proxy, enable redirects or supply Git credentials.

`SqlSkillBundleStore` writes scoped content-addressed bytes and the Registry draft in one
transaction. Registry publish/revoke remains explicit. `SqlRunSkillAccess` rechecks the
current Actor/Run grant and immutable Agent binding. GC retains draft, active and pinned
superseded Skill bytes until the corresponding revisions are revoked.

```python
from agent_runtime.storage.skill_access import SqlRunSkillAccess
from agent_runtime.storage.skill_bundles import SqlSkillBundleStore
from agent_runtime.storage.skill_extensions import SqlSkillExtensionResolver

skill_store = SqlSkillBundleStore(
    engine, schema, registry=registry, access=SqlRunSkillAccess(engine, schema)
)
await skill_store.install(actor, verified_bundle)
await registry.publish(actor, verified_bundle.spec.ref)
extensions = SqlSkillExtensionResolver(skill_store)
# Supply extensions to SqlMainHandlerFactory. It resolves the current Run's Agent only.
```

For SQL script tools, register `storage.skill_tool.SqlSkillScriptTool` in
`SqlToolHost.background_tools`, using the same `SqlToolCapacity` instance as the host.
It reloads the pinned Skill on each call, wraps the host's live lease/ACL, execution gate
and workspace/image authorizers, and reserves capacity atomically with the durable sandbox
intent. A final Skill revocation rejection rolls back both intent and capacity. Unknown
remote effects retain capacity until reconciliation proves the process tree stopped.

Supply a host-owned `SkillSandboxResolver` returning `SkillSandboxExecution`; it owns
SDK clients, per-execution sandbox isolation, TTL and cleanup on pre-dispatch rejection.
Bind the script path and an explicit `python` or `sh` interpreter in the trusted host;
model input may contain only bounded argv. Workspaces keep `noexec`: the interpreter
reads verified files. Low-level `InstalledSkillScriptTool` does not implement this SQL
capacity protocol and cannot be directly registered as a SQL background executor.
The host still supplies approval policy, resource versions, budgets, bounded result
sealing and independent maintenance. Skill instructions are lower-trust snapshot material
and cannot grant access. All execution services have explicit host lifetimes.

Local checks (run from the repository root):

```bash
PYTHONPATH=packages/agent-runtime/src uv run python scripts/verify_runtime_capability_registry.py
uv run --project packages/agent-runtime --no-sync --with opensandbox-server==0.2.3 \
  python scripts/verify_runtime_opensandbox.py --skill-worker \
  --output output/p04-03/r03-skill-combined.json
```

Both commands use only `aletai_wangxh` random schemas. The second starts disposable
HTTPS Git/CONNECT proxy and OpenSandbox services, installs the real pinned bundle into
SQL, and verifies ToolHost approval/budget/revocation gates, non-root execution, bounded
result sealing, cancellation, timeout, independent maintenance takeover and lost responses.
It cleans probe-owned schemas, processes and Docker resources. Its decision/approval
records are trusted test fixtures, not a real Provider conversation or approval UI test.
No application routes are switched by these SDK adapters; see the R03 review for results.

R03 accepted (2026-10-19): the full 15-case real Git/SQL/OpenSandbox/maintenance matrix
passed, including revocation after bundle load with no dispatch or capacity leak.
All 15 test schemas and probe-owned containers were cleaned, with no fallback cleanup.
Package regression: 1246 passed / 313 skipped; separate live SQL Skill/capacity regression:
6 passed. Full-package Ruff and strict mypy (208 source files), Schema drift, wheel build
and isolated wheel imports passed. Final evidence is local under
`output/p04-03/r03-skill-combined-final.json`; review section 24 records scope and limits.
R05 A2A and R06 combined acceptance remain separate, unfinished work packages.
