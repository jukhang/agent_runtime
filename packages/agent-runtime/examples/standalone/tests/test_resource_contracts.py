"""Independent host input boundaries need no database or Provider."""

from uuid import uuid4

import pytest
from centn_example.approvals import Approvals
from centn_example.resource_migrate import resource_schema
from centn_example.resources import ResourceCreate
from pydantic import ValidationError


@pytest.mark.parametrize("title", [" ", "../file.txt", "a\\b", "a\r\nb", "x\x00"])
def test_resource_title_cannot_be_a_path_or_header_sequence(title):
    with pytest.raises(ValidationError):
        ResourceCreate(session_id=uuid4(), idempotency_key="upload", title=title, text="data")


@pytest.mark.parametrize("text", [" ", "\x00", "中" * 349526])
def test_resource_text_is_bounded_by_utf8_bytes(text):
    with pytest.raises(ValidationError):
        ResourceCreate(session_id=uuid4(), idempotency_key="upload", title="资料.txt", text=text)


def test_resource_schema_is_not_runtime_schema_and_obeys_identifier_bound():
    assert resource_schema("isolated") == "isolated_host"
    with pytest.raises(ValueError):
        resource_schema("x" * 63)


def test_independent_host_imports_and_has_approval_and_delegation_ports():
    from centn_example.api import create_app
    from centn_example.delegation import Delegation
    from centn_example.host import Host

    assert callable(create_app) and callable(Approvals.resolve)
    assert callable(Host.maintenance) and callable(Delegation.collect)
