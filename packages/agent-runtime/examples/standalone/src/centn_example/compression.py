"""Opt-in assembly for Host.open; no environment loading or client ownership."""

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.context_runtime import ContextRuntime
from agent_runtime.contracts.model import ProviderCapabilities
from agent_runtime.providers.openai_compatible import OpenAIAdapterProfile, ProviderConnection
from agent_runtime.storage.compression import SqlContextCompression, SummaryResourceAccess
from agent_runtime.storage.context_access import SqlContextAccessVerifier
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.context_factory import SqlContextComponents
from agent_runtime.storage.context_snapshots import SqlContextSnapshotStore
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader


class ByteEstimator:
    def count(self, text: str) -> int:
        return len(text.encode())


def build_compressed_context(
    host,
    actor,
    lease,
    *,
    max_summary_input_tokens=None,
    summary_timeout_seconds=None,
):
    """Attach the returned components to MainHostPorts.context in both host paths."""
    if max_summary_input_tokens is None:
        max_summary_input_tokens = getattr(host.settings, "compression_max_input_tokens", 200_000)
    if summary_timeout_seconds is None:
        summary_timeout_seconds = getattr(host.settings, "compression_timeout_seconds", 60.0)
    history = SqlRuntimeSourceReader(
        host.engine,
        host.settings.schema,
        blobs=SqlContextBlobBackend(host.engine, host.settings.schema),
        optional_history=True,
    )

    class Reader:
        async def read(self, request, resource):
            if resource.resource_type in {"turn_input", "timeline_item", "interaction_input"}:
                return await history.read(request, resource)
            return await host.sources.read(request, resource)

    reader = Reader()

    async def connection(request):
        if request.model != host.model:
            raise ValueError("Compression model must match the host's pinned Qwen model")
        return ProviderConnection(
            base_url=host.settings.provider_url,
            api_key=host.settings.provider_key,
        )

    async def capabilities(request):
        return ProviderCapabilities(
            model=host.model.ref,
            streaming="supported",
            tool_calls="supported",
            structured_output="supported",
            thinking_control="supported",
            tools_with_structured_output="unknown",
            tools_with_thinking="supported",
            usage="supported",
            cancellation="supported",
            context_window=262144,
            max_output_tokens=32768,
            allowed_option_keys=("chat_template_kwargs",),
        )

    compression = SqlContextCompression(
        host.engine,
        host.settings.schema,
        actor=actor,
        lease=lease,
        reader=reader,
        resources=host.sources,
        estimator=ByteEstimator(),
        model=host.model,
        client=host.client,
        connections=connection,
        capabilities=capabilities,
        adapter=OpenAIAdapterProfile.qwen(),
        max_input_tokens=max_summary_input_tokens,
        timeout_seconds=summary_timeout_seconds,
    )
    return SqlContextComponents(
        runtime=ContextRuntime(
            reader=reader,
            estimator=ByteEstimator(),
            payloads=BlobContextPayloadStore(
                SqlContextBlobBackend(host.engine, host.settings.schema)
            ),
            snapshots=SqlContextSnapshotStore(host.engine, host.settings.schema),
            compression=compression,
        ),
        access=SqlContextAccessVerifier(
            host.engine,
            host.settings.schema,
            resources=SummaryResourceAccess(host.sources, compression),
        ),
    )
