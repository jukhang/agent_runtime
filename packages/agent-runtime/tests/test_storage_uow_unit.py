"""No-database invariants for the SQL unit of work (DS-P02-02).

These tests need the ``storage`` extra but no PostgreSQL: they pin the *structural* rules
that make the concurrency properties possible (single ``FOR UPDATE`` site, frozen failure
codes, registry boundaries).
"""

import ast
from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy", reason="requires agent-runtime[storage]")

from agent_runtime.contracts.error_codes import ERROR_CODES  # noqa: E402
from agent_runtime.storage.uow import (  # noqa: E402
    CAPACITY_UNAVAILABLE,
    CONTROL_PENDING,
    FENCED,
    FORBIDDEN,
    INVALID_INPUT,
    LOCK_ORDER,
    VERSION_CONFLICT,
    DomainRegistry,
    DomainResourceSpec,
    errors,  # noqa: E402
)

UOW_DIR = Path(__file__).resolve().parents[1] / "src/agent_runtime/storage/uow"


def test_for_update_is_confined_to_the_lock_and_operation_claim():
    """Row locks live in ``locks.py``; the only other one is the operation claim (P1-5)."""
    allowed = {"locks.py", "state.py"}
    offenders = []
    for path in sorted(UOW_DIR.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        if "FOR UPDATE" in text and path.name not in allowed:
            offenders.append(path.name)
    assert offenders == [], offenders
    assert "FOR UPDATE" in (UOW_DIR / "locks.py").read_text(encoding="utf-8")
    state_text = (UOW_DIR / "state.py").read_text(encoding="utf-8")
    assert state_text.count("FOR UPDATE") == 1, "only the operation claim may lock here"


def test_lock_order_documents_the_frozen_sequence():
    assert LOCK_ORDER[:3] == ("session_slot", "run", "authority")
    # No global table lock may serialize unrelated runs (Q46).
    assert "capability_grant" not in LOCK_ORDER
    assert LOCK_ORDER.index("command") < LOCK_ORDER.index("agent_message")
    assert LOCK_ORDER.index("mailbox_cursor") < LOCK_ORDER.index("system_capacity")
    assert LOCK_ORDER.index("system_capacity_reservation") < LOCK_ORDER.index("domain_resource")


def test_uow_methods_are_the_frozen_state_store_surface():
    from agent_runtime.ports import StateStore
    from agent_runtime.storage.uow import SqlStateUnitOfWork

    for method in (
        "commit_advance",
        "commit_pause",
        "commit_cancel",
        "commit_terminal",
        "resume",
        "apply_input",
        "commit_domain",
    ):
        assert hasattr(StateStore, method), method
        assert hasattr(SqlStateUnitOfWork, method), method


def test_every_classification_code_is_registered():
    for code in (
        FENCED,
        VERSION_CONFLICT,
        CONTROL_PENDING,
        FORBIDDEN,
        CAPACITY_UNAVAILABLE,
        INVALID_INPUT,
    ):
        assert code in ERROR_CODES, code


def test_classification_helpers_produce_stable_codes_and_categories():
    cases = [
        (errors.fenced, "FENCED", "concurrency"),
        (errors.version_conflict, "VERSION_CONFLICT", "concurrency"),
        (errors.control_pending, "CONTROL_PENDING", "concurrency"),
        (errors.forbidden, "FORBIDDEN", "authorization"),
        (errors.invalid_input, "INVALID_INPUT", "validation"),
    ]
    for helper, code, category in cases:
        with pytest.raises(Exception) as error:
            helper("detail")
        failure = error.value.failure  # type: ignore[attr-defined]
        assert failure.code == code
        assert failure.category == category
    with pytest.raises(Exception) as error:
        errors.capacity_unavailable("detail")
    assert error.value.failure.code == "CAPACITY_UNAVAILABLE"  # type: ignore[attr-defined]
    assert error.value.failure.category == "budget"  # type: ignore[attr-defined]
    assert error.value.failure.retryable is True  # type: ignore[attr-defined]


def test_domain_registry_refuses_unknown_tables_and_is_extensible():
    registry = DomainRegistry.default()
    assert registry.get("workspace") is not None
    assert registry.get("artifact") is None
    with pytest.raises(ValueError):
        DomainRegistry(
            (DomainResourceSpec("host_table", table="host_private", version_column="version"),)
        )
    extended = registry.extended(
        DomainResourceSpec("projection", table="projection", version_column="projection_revision")
    )
    assert set(extended.registered_types()) >= {"workspace", "projection"}


def test_state_module_never_opens_a_second_transaction_per_method():
    """Each public method owns exactly one ``session.begin()`` block."""
    tree = ast.parse((UOW_DIR / "state.py").read_text(encoding="utf-8"))
    class_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "SqlStateUnitOfWork"
    )
    public = [
        node
        for node in class_node.body
        if isinstance(node, ast.AsyncFunctionDef) and not node.name.startswith("_")
    ]
    names = {node.name for node in public}
    assert names == {
        "commit_advance",
        "commit_pause",
        "commit_cancel",
        "commit_terminal",
        "resume",
        "apply_input",
        "record_control",
        "record_output_seal",
        "commit_domain",
        "commit_answer",
        "commit_plan",
    }
    for function in public:
        if function.name in {"commit_answer", "commit_plan"}:
            filename = "answers.py" if function.name == "commit_answer" else "plans.py"
            delegate = ast.parse((UOW_DIR / filename).read_text(encoding="utf-8"))
            function = next(
                node
                for node in delegate.body
                if isinstance(node, ast.AsyncFunctionDef) and node.name == function.name
            )
        begins = [
            node
            for node in ast.walk(function)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "begin"
        ]
        assert len(begins) == 1, function.name


def test_no_global_authority_table_lock_remains():
    """Q46: the fence is the run row, never a table-wide lock."""
    import ast

    from agent_runtime.storage.uow import locks

    for path in sorted(UOW_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docstrings = {
            id(node.body[0].value)
            for node in ast.walk(tree)
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            and node.body
            and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
            and isinstance(node.body[0].value.value, str)
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if id(node) in docstrings:
                    continue
                assert "LOCK TABLE" not in node.value.upper(), path.name

    statements = locks.build_lock_statements("agent_runtime")
    for name in statements.__slots__:
        rendered = str(getattr(statements, name).text).upper()
        assert "LOCK TABLE" not in rendered, name


def test_every_lock_statement_is_schema_qualified():
    """P1-3: the standard default config has no search_path, so SQL must qualify."""
    import re

    from agent_runtime.storage.uow import locks
    from agent_runtime.storage.uow.schema import require_schema, scoped

    statements = locks.build_lock_statements("agent_runtime")
    for statement in statements.__slots__:
        text = str(getattr(statements, statement).text)
        if "{" in text:  # pragma: no cover - templates must not survive rendering
            raise AssertionError(f"{statement} was not rendered")
        for table in ("run", "work_item", "command", "capability_grant", "session_slot"):
            unqualified = re.findall(
                rf"\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+{table}\b", text, flags=re.I
            )
            if unqualified:
                raise AssertionError(f"{statement} references {table} without a schema")
    assert "agent_runtime.run" in str(statements.run.text)
    assert scoped("SELECT 1 FROM {t}run", "s").text == "SELECT 1 FROM s.run"
    with pytest.raises(ValueError):
        require_schema("Public; DROP TABLE run")


def test_uow_requires_an_explicit_schema():
    import inspect

    from agent_runtime.storage.uow import SqlStateUnitOfWork, SqlSystemCapacityStore

    state_params = list(inspect.signature(SqlStateUnitOfWork.__init__).parameters)
    capacity_params = list(inspect.signature(SqlSystemCapacityStore.__init__).parameters)
    assert state_params[:4] == ["self", "engine", "schema", "actor"]
    assert capacity_params[:3] == ["self", "engine", "schema"]
