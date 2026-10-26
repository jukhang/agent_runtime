"""Isolated reference HTTP transport; no execution is owned by HTTP connections."""

import asyncio
import errno
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote
from uuid import UUID

import anyio
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as SqlTimeoutError

from agent_runtime.contracts.events import ResyncFrame
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope, RecoveryOption
from agent_runtime.contracts.requests import (
    ControlRequest,
    InteractionRequest,
    StreamRequest,
    SubmitRequest,
)
from agent_runtime.contracts.tools import ApprovalDecision
from centn_example.config import Settings
from centn_example.host import Host
from centn_example.resources import ResourceCreate, ResourceRead
from centn_example.waits import ExternalResult

ASSETS = Path(__file__).parent / "ui"


def transient_dependency_error(exc):
    if isinstance(exc, DBAPIError):
        state = getattr(exc.orig, "sqlstate", "") or ""
        return exc.connection_invalidated or (isinstance(state, str) and state.startswith("08"))
    if isinstance(exc, (SqlTimeoutError, TimeoutError)):
        return True
    return isinstance(exc, OSError) and exc.errno in {
        errno.ECONNREFUSED,
        errno.ECONNRESET,
        errno.EHOSTUNREACH,
        errno.ENETDOWN,
        errno.ENETUNREACH,
        errno.ETIMEDOUT,
        errno.EPIPE,
    }


def session_json(row):
    return jsonable_encoder(
        {
            "id": row.id,
            "title": row.title,
            "createdAt": row.created_at,
            "updatedAt": row.updated_at,
        }
    )


def create_app(settings=None):
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app):
        host = Host(settings)
        try:
            await host.initialize()
            app.state.host = host
            yield
        finally:
            await host.aclose()

    app = FastAPI(title="centn Runtime independent example", lifespan=lifespan)

    @app.exception_handler(DBAPIError)
    @app.exception_handler(SqlTimeoutError)
    @app.exception_handler(OSError)
    async def dependency_unavailable(request, exc):
        if not transient_dependency_error(exc):
            raise exc
        # Transport uncertainty is not a Run failure or permission to replay effects.
        failure = FailureEnvelope(
            code="DEPENDENCY_UNAVAILABLE",
            category="storage",
            retryable=True,
            public_message="依赖服务暂时不可用，当前执行状态尚未确认。"
            "请保留会话与请求编号，稍后重试读取；提交重试须使用原幂等键，勿新建重复任务。",
            recovery_options=(RecoveryOption(action="retry_same", label="沿用原请求编号重试"),),
        )
        return JSONResponse(
            status_code=503,
            headers={"Retry-After": "1"},
            content={
                "code": failure.code,
                "message": failure.public_message,
                "retryable": failure.retryable,
                "recoveryOptions": [
                    option.model_dump(mode="json", by_alias=True)
                    for option in failure.recovery_options
                ],
            },
        )

    async def actor(request: Request, authorization: str = Header(default="")):
        supplied = authorization.removeprefix("Bearer ")
        if (
            not authorization.startswith("Bearer ")
            or len(supplied) > 4096
            or not secrets.compare_digest(supplied, settings.token)
        ):
            raise HTTPException(401, "Invalid host token")
        return request.app.state.host.actor()

    principal_dependency = Depends(actor)

    @app.exception_handler(AgentRuntimeError)
    async def failure(request, exc):
        return JSONResponse(
            status_code=409,
            content={
                "code": exc.failure.code,
                "message": exc.failure.public_message,
                "recoveryOptions": [
                    option.model_dump(mode="json", by_alias=True)
                    for option in exc.failure.recovery_options
                ],
            },
        )

    @app.get("/api/config")
    async def config(request: Request, principal=principal_dependency):
        host = request.app.state.host
        return {
            "agent": host.installed.ref.model_dump(mode="json", by_alias=True),
            "name": host.installed.agent.name,
            "thinking": settings.thinking,
        }

    @app.get("/api/sessions")
    async def sessions(request: Request, principal=principal_dependency):
        return [session_json(row) for row in await request.app.state.host.sessions.list(principal)]

    @app.post("/api/sessions", status_code=201)
    async def create_session(request: Request, principal=principal_dependency):
        return session_json(await request.app.state.host.sessions.create(principal))

    @app.post("/api/turns", status_code=202)
    async def submit(body: SubmitRequest, request: Request, principal=principal_dependency):
        accepted = await request.app.state.host.runtime.submit(principal, body)
        return accepted.accepted.model_dump(mode="json", by_alias=True)

    @app.post("/api/interactions", status_code=202)
    async def interact(body: InteractionRequest, request: Request, principal=principal_dependency):
        receipt = await request.app.state.host.runtime.interact(principal, body)
        return receipt.model_dump(mode="json", by_alias=True)

    @app.post("/api/sessions/{session_id}/runs/{run_id}/external-result", status_code=201)
    async def external_result(
        session_id: UUID,
        run_id: UUID,
        body: ExternalResult,
        request: Request,
        principal=principal_dependency,
    ):
        value = await request.app.state.host.external_wait.signal(
            principal, session_id, run_id, body.text
        )
        return value.model_dump(mode="json", by_alias=True)

    @app.get("/api/sessions/{session_id}/approvals")
    async def list_approvals(session_id: UUID, request: Request, principal=principal_dependency):
        return await request.app.state.host.approvals.list(principal, session_id)

    @app.post("/api/sessions/{session_id}/approvals")
    async def decide_approval(
        session_id: UUID, body: ApprovalDecision, request: Request, principal=principal_dependency
    ):
        return await request.app.state.host.approvals.resolve(principal, session_id, body)

    @app.post("/api/resources", status_code=201)
    async def create_resource(request: Request, principal=principal_dependency):
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > 2 * 1048576:
                raise HTTPException(413, "Resource request exceeds 2 MiB")
            body.extend(chunk)
        try:
            value = ResourceCreate.model_validate_json(body)
        except ValidationError:
            raise HTTPException(422, "Invalid resource data") from None
        result = await request.app.state.host.resources.create(principal, value)
        return result.model_dump(mode="json", by_alias=True)

    @app.get("/api/sessions/{session_id}/resources")
    async def list_resources(session_id: UUID, request: Request, principal=principal_dependency):
        return [
            item.model_dump(mode="json", by_alias=True)
            for item in await request.app.state.host.resources.list(principal, session_id)
        ]

    @app.post("/api/resources/preview")
    async def preview_resource(
        body: ResourceRead, request: Request, principal=principal_dependency
    ):
        view, content = await request.app.state.host.resources.get(principal, body.resource_ref)
        if view.mime == "image/png":
            return Response(
                content,
                media_type="image/png",
                headers={
                    "Cache-Control": "no-store",
                    "X-Content-Type-Options": "nosniff",
                    "Content-Security-Policy": "default-src 'none'; sandbox",
                    "ETag": '"' + view.digest + '"',
                },
            )
        return JSONResponse(
            {
                "resource": view.model_dump(mode="json", by_alias=True),
                "text": content.decode("utf-8"),
            },
            headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
        )

    @app.post("/api/resources/download")
    async def download_resource(
        body: ResourceRead, request: Request, principal=principal_dependency
    ):
        view, content = await request.app.state.host.resources.get(principal, body.resource_ref)
        return Response(
            content,
            media_type=view.mime,
            headers={
                "Content-Disposition": "attachment; filename=resource.txt; filename*=UTF-8''"
                + quote(view.title, safe=""),
                "X-Content-Type-Options": "nosniff",
                "Cache-Control": "no-store",
                "Content-Security-Policy": "default-src 'none'; sandbox",
                "ETag": '"' + view.digest + '"',
            },
        )

    @app.post("/api/resources/revoke", status_code=204)
    async def revoke_resource(body: ResourceRead, request: Request, principal=principal_dependency):
        await request.app.state.host.resources.revoke(principal, body.resource_ref)
        return Response(status_code=204)

    @app.get("/api/runtime/sessions/{session_id}/snapshot")
    async def snapshot(session_id: UUID, request: Request, principal=principal_dependency):
        value = await request.app.state.host.backend.snapshot(principal, session_id)
        return value.model_dump(mode="json", by_alias=True)

    @app.post("/api/sessions/{session_id}/controls")
    async def control(
        session_id: UUID, body: ControlRequest, request: Request, principal=principal_dependency
    ):
        value = await request.app.state.host.backend.control(principal, session_id, body)
        return value.model_dump(mode="json", by_alias=True)

    @app.get("/api/sessions/{session_id}/runs/{run_id}/result")
    async def result(
        session_id: UUID, run_id: UUID, request: Request, principal=principal_dependency
    ):
        handle = request.app.state.host.runtime.attach(principal, session_id, run_id)
        value = await handle.result(wait_timeout=5)
        return value.model_dump(mode="json", by_alias=True)

    @app.get("/api/sessions/{session_id}/events")
    async def events(
        session_id: UUID,
        request: Request,
        after: str = "0",
        last_event_id: str | None = Header(default=None),
        principal=principal_dependency,
    ):
        current = StreamRequest(session_id=session_id, after_seq=after, last_event_id=last_event_id)
        reader = request.app.state.host.backend

        async def read_page():
            # Starlette's disconnect cancel scope must not repeatedly cancel DB
            # rollback/connection return. The host applies a SQL statement timeout.
            with anyio.CancelScope(shield=True):
                return await reader.read_page(principal, current, limit=64)

        initial = await read_page()

        async def stream():
            nonlocal current
            page = initial
            while not await request.is_disconnected():
                if isinstance(page, ResyncFrame):
                    yield "data: " + page.model_dump_json(by_alias=True) + "\n\n"
                    return
                if page.cursor != current.after_seq:
                    yield f"id: {page.cursor}\ndata: {page.model_dump_json(by_alias=True)}\n\n"
                    current = current.model_copy(
                        update={"after_seq": page.cursor, "last_event_id": None}
                    )
                else:
                    yield ": keepalive\n\n"
                    await asyncio.sleep(0.25)
                page = await read_page()

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/")
    async def index():
        return FileResponse(ASSETS / "index.html")

    @app.get("/ui/{filename}")
    async def asset(filename: str):
        if filename not in {"app.mjs", "journal.mjs", "composer.mjs", "style.css"}:
            raise HTTPException(404)
        return FileResponse(ASSETS / filename)

    return app
