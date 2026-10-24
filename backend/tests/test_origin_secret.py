from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from starlette.websockets import WebSocketDisconnect

from astrq.api.app import create_app
from astrq.core.config import load_settings

ROOT = Path(__file__).resolve().parents[2]
SECRET = "s3cr3t-origin-value"


def test_origin_secret_blocks_direct_access_and_gzip(tmp_path: Path) -> None:
    s = load_settings(ROOT / "config" / "settings.yaml", use_dotenv=False)
    s.storage.data_dir = str(tmp_path)
    s.api.frontend_dist = str(tmp_path / "nope")
    s.api.origin_secret = SecretStr(SECRET)
    with TestClient(create_app(s)) as c:
        assert c.get("/health").status_code == 200  # health checks / keep-alive stay open
        assert c.get("/api/status").status_code == 403
        assert c.get("/api/status", headers={"X-Astrq-Origin": "wrong"}).status_code == 403
        ok = c.get(
            "/api/tools/large_order_radar/view",
            headers={"X-Astrq-Origin": SECRET, "Accept-Encoding": "gzip"},
        )
        assert ok.status_code == 200
        with pytest.raises(WebSocketDisconnect), c.websocket_connect("/ws") as ws:
            ws.receive_json()
        with c.websocket_connect("/ws", headers={"X-Astrq-Origin": SECRET}) as ws:
            assert ws.receive_json()["type"] == "hello"
        big = c.get("/api/tools", headers={"X-Astrq-Origin": SECRET, "Accept-Encoding": "gzip"})
        assert big.headers.get("content-encoding") == "gzip"
