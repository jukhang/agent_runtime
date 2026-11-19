# Bounded Model Working Sets

The durable transcript is not the model input. ContextRuntime retains versioned
source identities, ACL checks and complete original tool outcomes in storage.

Hosts may supply ToolResultWindow policies for explicitly re-readable tools. Only
older successful full results acquire an alternative representation; the latest
read stays complete. Failed or uncertain effects are never offloaded. The runtime
uses an alternative only under required-input pressure and only if its complete
framing reduces the estimated input. Every change is visible as an
offloaded_retrievable source reason and an original-content digest. Indexes are
not evidence; exact quotes must be re-read and verified by domain tools.

Hosts may explicitly retire successful results once a durable downstream state
owns their information (for example a saved batch note or validated report).
Such results are optional in the current working set and may leave model input,
while the immutable journal and original outcomes remain intact. This is opt-in:
ToolResultWindow defaults to protecting the latest result and never retiring any.
Failed receipts cannot be retired. A host must keep unresolved inputs and the
actual recovery evidence needed by its completion verifier visible. A reference
index must never be mistaken for an inspected original excerpt.

Compression is optional extraction, not lossless semantic memory. A summary that
does not reduce the complete candidate request cannot replace its source. Hosts
using SqlContextCompression should wire compression_no_gain=compressor.record_no_gain
to preserve no-gain feedback across claims. These observations are scoped to a Run,
source/version, task, prompt and model. Unknown usage and invalid extraction still
require explicit recovery; they must not become fabricated zero-cost successes.

Required sources without an authorized smaller representation remain protected.
The runtime cannot fit an arbitrarily large indivisible input. Hosts must expose
bounded read operations and cross-resource re-reading rather than silently truncate.
