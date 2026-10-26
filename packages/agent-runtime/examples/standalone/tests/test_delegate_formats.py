"""Model-facing completion constraints do not replace commit-time authorization."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
from centn_example.delegation import DELEGATE_REVIEW_SCHEMA, Delegation


@pytest.mark.asyncio
@pytest.mark.parametrize("included", [True, False])
async def test_partial_review_only_exposes_pinned_evidence_and_sealed_items(included):
    item_id = uuid4()
    pinned = {"resourceType": "turn_input", "resourceId": str(uuid4()), "resourceVersion": "0"}

    class Resource:
        resource_version = "0"

        def model_dump(self, **kwargs):
            return pinned

    class Session:
        async def execute(self, statement, params):
            assert params["run"] == request.run_id
            return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [item_id]))

    @asynccontextmanager
    async def sessions():
        yield Session()

    host = SimpleNamespace(
        engine=None,
        settings=SimpleNamespace(schema="example"),
        resources=SimpleNamespace(sessions=sessions),
    )
    request = SimpleNamespace(
        output_schema=DELEGATE_REVIEW_SCHEMA, scope_id="example", run_id=uuid4()
    )
    snapshot = SimpleNamespace(
        sources=[
            SimpleNamespace(resource=Resource(), disposition="included" if included else "omitted")
        ]
    )
    result = await Delegation(host).output_format(request, snapshot)
    properties = result["json_schema"]["schema"]["$defs"]["FinishPayload"]["properties"]
    assert properties["outcome"] == {"enum": ["partial"]}
    assert properties["evidenceRefs"]["items"] == ({"enum": [pinned]} if included else False)
    assert properties["finalItemIds"]["items"] == {"enum": [str(item_id)]}
    assert properties["finalItemIds"]["minItems"] == 1
