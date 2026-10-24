"""REST + WebSocket API. Read-only market analysis: there is no trading endpoint."""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.types import ASGIApp, Receive, Scope, Send

from astrq.core import fmt
from astrq.core.config import Settings
from astrq.core.metrics import METRICS
from astrq.core.models import Severity
from astrq.core.platform import Platform


class EnabledBody(BaseModel):
    enabled: bool


class OriginSecretMiddleware:
    """Reject requests that did not come through our Cloudflare domain.

    Cloudflare adds `X-Astrq-Origin: <secret>` (Transform Rule) to traffic for the custom
    domain; direct hits on the host's default URL (e.g. *.onrender.com) lack it and get
    403. Pure ASGI so WebSocket handshakes are covered too. /health stays open for the
    host's health checks and the keep-alive cron.
    """

    OPEN_PATHS = frozenset({"/health"})

    def __init__(self, app: ASGIApp, secret: str) -> None:
        self.app = app
        self.secret = secret.encode()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket") or scope["path"] in self.OPEN_PATHS:
            await self.app(scope, receive, send)
            return
        got = dict(scope.get("headers") or []).get(b"x-astrq-origin", b"")
        if hmac.compare_digest(got, self.secret):
            await self.app(scope, receive, send)
            return
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        await PlainTextResponse("Forbidden", status_code=403)(scope, receive, send)


def create_app(settings: Settings) -> FastAPI:
    platform = Platform(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await platform.start()
        try:
            yield
        finally:
            await platform.stop()

    app = FastAPI(title="ASTRQ Stock Tools Platform", version="0.1.0", lifespan=lifespan)
    app.state.platform = platform
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.api.cors_origins,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )
    # Compress JSON (the radar view is large and free hosts meter egress).
    app.add_middleware(GZipMiddleware, minimum_size=1024)
    if settings.api.origin_secret is not None:
        app.add_middleware(
            OriginSecretMiddleware, secret=settings.api.origin_secret.get_secret_value()
        )
    frame_policy = "frame-ancestors " + " ".join(settings.api.frame_ancestors or ["'none'"])

    @app.middleware("http")
    async def embed_policy(request: Request, call_next: Any) -> Any:
        # Only the configured sites may put the radar in an <iframe> (anti-clickjacking).
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = frame_policy
        return response

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    async def ready() -> dict[str, Any]:
        ok = platform.started and platform.provider.status.connected and not platform.feed_error
        if not ok:
            raise HTTPException(503, detail={"ready": False, "feed_error": platform.feed_error})
        return {"ready": True}

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        return METRICS.render_prometheus()

    @app.get("/api/status")
    async def status() -> dict[str, Any]:
        return {**platform.status(), "disclaimer": fmt.DISCLAIMER_VI}

    @app.get("/api/universe")
    async def universe() -> dict[str, Any]:
        u = platform.universe
        return {
            "meta": u.meta.model_dump(exclude={"instruments"}),
            "instruments": [i.model_dump() for i in u.instruments.values()],
        }

    @app.get("/api/signals")
    async def signals(
        tool_id: str | None = None,
        symbol: str | None = None,
        side: str | None = None,
        min_severity: Severity | None = None,
        limit: int = Query(200, le=2000),
    ) -> list[dict[str, Any]]:
        rows = platform.store.query(tool_id, symbol, side, min_severity, limit)
        return [s.model_dump(mode="json") for s in rows]

    @app.get("/api/alerts")
    async def alerts() -> list[dict[str, Any]]:
        return [a.model_dump(mode="json") for a in reversed(platform.router.history)]

    @app.get("/api/summary")
    async def summary() -> dict[str, Any]:
        return {"text": platform.last_summary}

    @app.get("/api/tools")
    async def tools() -> list[dict[str, Any]]:
        out = []
        for r in platform.runners.values():
            out.append(
                {
                    "manifest": r.tool.manifest().model_dump(),
                    "config": r.ctx.config.model_dump(),
                    "status": r.status_dict(),
                    "control_enabled": settings.api.allow_tool_control,
                }
            )
        return out

    @app.post("/api/tools/{tool_id}/enabled")
    async def set_enabled(tool_id: str, body: EnabledBody) -> dict[str, Any]:
        if not settings.api.allow_tool_control:
            raise HTTPException(403, "tool control is disabled on this deployment")
        if tool_id not in platform.runners:
            raise HTTPException(404, "unknown tool")
        platform.set_tool_enabled(tool_id, body.enabled)
        return platform.runners[tool_id].status_dict()

    @app.get("/api/tools/{tool_id}/view")
    async def tool_view(tool_id: str, request: Request) -> dict[str, Any]:
        r = platform.runners.get(tool_id)
        if r is None:
            raise HTTPException(404, "unknown tool")
        fn = getattr(r.tool, "view", None)
        if not callable(fn):
            raise HTTPException(404, "tool has no view")
        result: dict[str, Any] = fn(
            platform.store.by_tool(tool_id), r.ctx, dict(request.query_params)
        )
        return result

    @app.websocket("/ws")
    async def ws(websocket: WebSocket) -> None:
        await websocket.accept()
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=2000)

        async def listener(msg: dict[str, Any]) -> None:
            # slow client: drop; it resyncs from REST
            with contextlib.suppress(asyncio.QueueFull):
                q.put_nowait(msg)

        platform.listeners.append(listener)
        try:
            await websocket.send_text(json.dumps({"type": "hello", "status": platform.status()}))
            while True:
                msg = await q.get()
                await websocket.send_text(json.dumps(msg, default=str))
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            with contextlib.suppress(ValueError):
                platform.listeners.remove(listener)

    dist = settings.path(settings.api.frontend_dist)
    if dist.exists() and (dist / "index.html").exists():
        app.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")

        @app.get("/{path:path}", include_in_schema=False)
        async def spa(path: str) -> FileResponse:
            f = dist / path
            if path and f.is_file() and dist in f.resolve().parents:
                return FileResponse(f)
            return FileResponse(dist / "index.html")

    return app
