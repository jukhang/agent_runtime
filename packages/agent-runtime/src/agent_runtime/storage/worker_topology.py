"""Role-specific SQL worker assembly for a separately supervised host process."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.work_claims import ChildClaimedWork, WorkClaimRequest
from agent_runtime.storage.host_worker import (
    ApprovalWakeScanner,
    ChildHandlerFactory,
    MainHandlerFactory,
    SqlClaimActorResolver,
    SqlClaimChildHandler,
    SqlClaimMainHandler,
    SqlWorkerRecoverySink,
    SqlWorkerService,
)
from agent_runtime.storage.recovery import SqlRecovery
from agent_runtime.storage.scheduler import SqlScheduler
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.worker import (
    ChildErrorSink,
    ChildLeaseHandle,
    ChildWorkHandler,
    WorkerConfig,
    WorkerRunner,
    WorkHandler,
)


class SqlWorkerTopology:
    """Build one role per OS process; all roles share SQL, never Python state."""

    def __init__(self, engine: AsyncEngine, schema: str, *, scope_id: str) -> None:
        if not scope_id:
            raise ValueError("worker scope must be explicit")
        self._engine = engine
        self._schema = require_schema(schema)
        self._scope_id = scope_id

    def _request(self, request: WorkClaimRequest, lanes: frozenset[str]) -> None:
        if request.scope_id != self._scope_id or set(request.lanes) != lanes:
            raise ValueError("worker role must have only its declared scope and lanes")

    def _service(
        self,
        request: WorkClaimRequest,
        *,
        handler: WorkHandler,
        config: WorkerConfig | None,
        main: SqlClaimMainHandler | None = None,
        child: ChildWorkHandler | None = None,
        child_errors: ChildErrorSink | None = None,
        approval_wakes: ApprovalWakeScanner | None = None,
        recovery_interval_seconds: float = 30.0,
    ) -> SqlWorkerService:
        recovery = SqlRecovery(self._engine, self._schema)
        sessions = async_sessionmaker(self._engine, expire_on_commit=False)

        async def child_failure(
            work: ChildClaimedWork, lease: ChildLeaseHandle, error: BaseException
        ) -> None:
            expired = False
            budget = (
                isinstance(error, AgentRuntimeError) and error.failure.code == "BUDGET_EXHAUSTED"
            )
            if not budget:
                async with sessions() as db:
                    expired = bool(
                        (
                            await db.execute(
                                scoped(
                                    "SELECT EXISTS(SELECT 1 FROM {t}run WHERE scope_id=:scope "
                                    "AND id=:run AND parent_run_id=:parent AND root_run_id=:parent "
                                    "AND deadline_at<=clock_timestamp())",
                                    self._schema,
                                ),
                                {
                                    "scope": work.lease.scope_id,
                                    "run": work.lease.run_id,
                                    "parent": work.root_run_id,
                                },
                            )
                        ).scalar_one()
                    )
            if budget or expired:
                # The handler has stopped; an expired execution grant must not
                # prevent fenced recovery from recording its budget failure.
                await recovery.handle_worker_failure(
                    work.model_copy(update={"lease": lease.current()}), error
                )
                return
            assert child_errors is not None
            await child_errors(work, lease, error)

        runner = WorkerRunner(
            SqlScheduler(self._engine, self._schema),
            request,
            handler,
            config=config,
            main_handler=main,
            child_handler=child,
            child_error_sink=child_failure if child_errors is not None else None,
            error_sink=SqlWorkerRecoverySink(recovery),
        )
        return SqlWorkerService(
            runner,
            recovery,
            scope_id=self._scope_id,
            approval_wakes=approval_wakes,
            recovery_interval_seconds=recovery_interval_seconds,
        )

    def main(
        self,
        request: WorkClaimRequest,
        *,
        handlers: MainHandlerFactory,
        config: WorkerConfig | None = None,
    ) -> SqlWorkerService:
        self._request(request, frozenset({"main", "control"}))
        if request.maintenance_kinds:
            raise ValueError("main process cannot claim maintenance work")
        claim = SqlClaimMainHandler(SqlClaimActorResolver(self._engine, self._schema), handlers)

        async def unsupported(work: object) -> None:
            raise ValueError("main process received non-main work")

        return self._service(request, handler=unsupported, main=claim, config=config)

    def child(
        self,
        request: WorkClaimRequest,
        *,
        handlers: ChildHandlerFactory,
        errors: ChildErrorSink,
        config: WorkerConfig | None = None,
    ) -> SqlWorkerService:
        self._request(request, frozenset({"child"}))
        if request.maintenance_kinds:
            raise ValueError("child process cannot claim maintenance work")
        claim = SqlClaimChildHandler(self._engine, self._schema, handlers)

        async def unsupported(work: object) -> None:
            raise ValueError("child process received non-child work")

        return self._service(
            request,
            handler=unsupported,
            child=claim,
            child_errors=errors,
            config=config,
        )

    def maintenance(
        self,
        request: WorkClaimRequest,
        *,
        execution: WorkHandler,
        approval_wakes: ApprovalWakeScanner,
        config: WorkerConfig | None = None,
        recovery_interval_seconds: float = 30.0,
    ) -> SqlWorkerService:
        self._request(request, frozenset({"maintenance"}))
        if set(request.maintenance_kinds) != {"reconcile_execution"}:
            raise ValueError("maintenance process must supervise executions")
        return self._service(
            request,
            handler=execution,
            approval_wakes=approval_wakes,
            config=config,
            recovery_interval_seconds=recovery_interval_seconds,
        )


__all__ = ["SqlWorkerTopology"]
