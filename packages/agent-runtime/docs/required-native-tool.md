# Required Native Tool Binding

Hosts may set `ContextRequest.required_tool` to a pinned capability already in
`tools`. Default `None` preserves automatic native tool selection. The binding is
stored in the immutable Context payload and covered by its digest. The trusted
OpenAI input resolver loads it after access and snapshot verification; the adapter
sends a named native `tool_choice`. Raw provider options cannot override it.

The Loop accepts this option only in native mode and checks the completed
decision before persisting an answer. A provider that ignores the choice fails
closed with `INCOMPLETE_MODEL_OUTPUT`, not a successful textual substitute.
This is not tool authorization: ToolHost policy, approval, leases, budget and
transaction checks still apply. Hosts must clear the binding for subsequent
free-response stages. No domain rules or prose-to-tool conversion are added.

The adapter normalizes raw `stop` only when a named required tool has exactly one
matching native call and fully parsed object arguments. The raw reason is retained
in the Provider finish event. Unbound stop-with-arguments, wrong names, malformed
JSON and truncated calls retain their existing rejection semantics.

Regression coverage: `tests/test_required_tool.py`, Context resolver, Loop and
OpenAI adapter suites. No database migration is required.
