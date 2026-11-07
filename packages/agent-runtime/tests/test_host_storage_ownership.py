import pytest

from agent_runtime.storage.migrate import MigrationPreflightError, validate_host_relations


@pytest.mark.parametrize("name", ["run", "alembic_version", "doc_*", "host;DROP", ""])
def test_host_cannot_claim_runtime_or_wildcard_identities(name):
    with pytest.raises(MigrationPreflightError):
        validate_host_relations(frozenset({name}))


def test_disjoint_explicit_host_identities_are_allowed():
    validate_host_relations(frozenset({"doc_workflow", "centn_document_alembic"}))
    validate_host_relations(frozenset())
