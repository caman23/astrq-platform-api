from __future__ import annotations

import time
from pathlib import Path

from fastapi.testclient import TestClient

from astrq.api.app import create_app
from astrq.core.config import load_settings

ROOT = Path(__file__).resolve().parents[2]


def test_api_end_to_end(tmp_path: Path) -> None:
    # ignore the developer's personal .env (it may point at a real feed)
    s = load_settings(ROOT / "config" / "settings.yaml", use_dotenv=False)
    s.provider.synthetic.speed = 400  # ~1 trading minute per 0.15 s
    s.storage.data_dir = str(tmp_path)
    s.api.frontend_dist = str(tmp_path / "nope")
    app = create_app(s)
    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok"}
        csp = client.get("/health").headers["Content-Security-Policy"]
        assert csp.startswith("frame-ancestors 'self'") and "https://*.astrq.io.vn" in csp
        deadline = time.time() + 20
        view: dict = {"symbols": []}
        while time.time() < deadline:
            view = client.get("/api/tools/large_order_radar/view").json()
            if len(view["symbols"]) >= 3:
                break
            time.sleep(0.3)
        assert len(view["symbols"]) >= 3, "radar should light up on synthetic data"
        sym = view["symbols"][0]
        order = sym["orders"][0]
        assert {"started_at", "last_at", "total_value", "n_trades", "side"} <= set(order)
        assert order["trades"], "latest orders carry child trades for hover details"

        detail = client.get("/api/tools/large_order_radar/view", params={"symbol": sym["symbol"]})
        assert detail.json()["symbols"][0]["symbol"] == sym["symbol"]

        st = client.get("/api/status").json()
        assert st["provider"]["connected"] and "khuyến nghị" in st["disclaimer"]
        assert st["tick_rate"] >= 0 and st["universe"]["verified"] is False

        tools = client.get("/api/tools").json()
        assert {t["manifest"]["id"] for t in tools} == {"large_order_radar", "tick_counter"}
        r = client.post("/api/tools/tick_counter/enabled", json={"enabled": False})
        assert r.json()["enabled"] is False

        assert (
            client.get("/api/signals", params={"min_severity": "alert", "limit": 5}).status_code
            == 200
        )
        assert "astrq_ticks_total" in client.get("/metrics").text
        assert client.get("/ready").status_code == 200

        with client.websocket_connect("/ws") as ws:
            hello = ws.receive_json()
            assert hello["type"] == "hello"
            kinds = {ws.receive_json()["type"] for _ in range(5)}
            assert kinds & {"signal", "status", "alert"}
