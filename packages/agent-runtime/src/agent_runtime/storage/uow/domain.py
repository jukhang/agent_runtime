"""Same-database domain resource registry for ``commit_domain`` (requirement 7).

A domain commit may only join the run's transaction when the resource lives in the same
database *and* has a mutable version column this adapter can CAS. Anything else is refused
and must use the external idempotency protocol: the UoW never fakes local atomicity for a
resource it cannot lock and version-check.

The built-in registry covers the versioned resources of the frozen catalog. Hosts may pass
a wider registry, but a resource type is never resolvable unless it is registered.
"""

from dataclasses import dataclass

from agent_runtime.storage.uow import locks


@dataclass(frozen=True, slots=True)
class DomainResourceSpec:
    resource_type: str
    table: str
    version_column: str
    description: str = ""


DEFAULT_RESOURCES: tuple[DomainResourceSpec, ...] = (
    DomainResourceSpec(
        "workspace",
        table="workspace",
        version_column="revision",
        description="workspace revision counter",
    ),
    DomainResourceSpec(
        "projection",
        table="projection",
        version_column="projection_revision",
        description="session projection revision",
    ),
    DomainResourceSpec(
        "budget_allocation",
        table="budget_allocation",
        version_column="version",
        description="budget allocation CAS counter",
    ),
)


class DomainRegistry:
    """Immutable mapping from ``ResourceRef.resource_type`` to a lockable table."""

    def __init__(self, resources: tuple[DomainResourceSpec, ...] = DEFAULT_RESOURCES) -> None:
        table_names = {spec.table for spec in resources}
        if table_names - set(locks.DOMAIN_TABLES):
            unknown = sorted(table_names - set(locks.DOMAIN_TABLES))
            raise ValueError(f"domain registry references unknown tables: {unknown}")
        self._resources = {spec.resource_type: spec for spec in resources}

    @classmethod
    def default(cls) -> "DomainRegistry":
        return cls()

    def get(self, resource_type: str) -> DomainResourceSpec | None:
        return self._resources.get(resource_type)

    def registered_types(self) -> tuple[str, ...]:
        return tuple(sorted(self._resources))

    def extended(self, *resources: DomainResourceSpec) -> "DomainRegistry":
        """Return a new registry with extra same-database resources."""
        return DomainRegistry((*self._resources.values(), *resources))


__all__ = [
    "DEFAULT_RESOURCES",
    "DomainRegistry",
    "DomainResourceSpec",
]
