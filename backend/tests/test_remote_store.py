"""R2 (S3-compatible) client: SigV4 signing checked against AWS's published examples,
and the sync round-trip against an in-memory fake bucket."""

from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx

from astrq.core.config import RemoteStorageConfig
from astrq.core.remote_store import EMPTY_SHA256, R2Client, RemoteSync, sign_v4
from astrq.providers.recorded_baseline import RecordedBaselineProvider

AK, SK = "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
T = datetime(2013, 5, 24, tzinfo=UTC)


def _sig(h: dict[str, str]) -> str:
    return h["Authorization"].rsplit("Signature=", 1)[1]


def test_sigv4_get_object_example() -> None:
    # AWS S3 docs, "Example: GET Object"
    h = sign_v4(
        "GET",
        httpx.URL("https://examplebucket.s3.amazonaws.com/test.txt"),
        {"range": "bytes=0-9"},
        EMPTY_SHA256,
        AK,
        SK,
        "us-east-1",
        T,
    )
    assert "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date" in h["Authorization"]
    assert _sig(h) == "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"


def test_sigv4_list_objects_example() -> None:
    # AWS S3 docs, "Example: GET Bucket (List Objects)"
    h = sign_v4(
        "GET",
        httpx.URL("https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J"),
        {},
        EMPTY_SHA256,
        AK,
        SK,
        "us-east-1",
        T,
    )
    assert _sig(h) == "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"


def _fake_bucket(store: dict[str, bytes]) -> httpx.MockTransport:
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=key/")
        key = req.url.path.split("/bkt/", 1)[1]
        if req.method == "PUT":
            store[key] = req.content
            return httpx.Response(200)
        return httpx.Response(200, content=store[key]) if key in store else httpx.Response(404)

    return httpx.MockTransport(handler)


async def test_sync_round_trip(tmp_path: Path) -> None:
    cfg = RemoteStorageConfig(
        kind="r2",
        endpoint="https://acct.r2.cloudflarestorage.com",
        bucket="bkt",
        access_key_id="key",
        secret_access_key="secret",
    )
    store: dict[str, bytes] = {}
    async with httpx.AsyncClient(transport=_fake_bucket(store)) as http:
        # host A records a day and uploads
        a = tmp_path / "a"
        (a / "ticks").mkdir(parents=True)
        tick = (
            '{"event_type":"tick","symbol":"FPT","value":20000000.0,"session":"continuous_am",'
            '"is_negotiated":false}\n'
        )
        for d in ("2026-10-20", "2026-10-21", "2026-10-22"):
            (a / "ticks" / f"{d}.jsonl").write_text(tick * 3)
        (a / "signals").mkdir()
        (a / "signals" / "2026-10-22.jsonl").write_text('{"x":1}\n')
        sync_a = RemoteSync(R2Client(cfg, http), a)
        for d in ("2026-10-20", "2026-10-21", "2026-10-22"):
            await sync_a.upload_day_summary(date.fromisoformat(d))
        await sync_a.upload_signals(date(2026, 10, 22))
        assert sync_a.last_error is None
        assert set(json.loads(gzip.decompress(store["astrq/baselines/summaries.json.gz"]))) == {
            "2026-10-20",
            "2026-10-21",
            "2026-10-22",
        }

        # host B starts empty (fresh container), restores, and has warm baselines
        b = tmp_path / "b"
        sync_b = RemoteSync(R2Client(cfg, http), b)
        await sync_b.restore(date(2026, 10, 22))
        assert (b / "signals" / "2026-10-22.jsonl").read_text() == '{"x":1}\n'
        prov = RecordedBaselineProvider(b / "ticks", b / "baselines", 10, min_days=3)
        bl = await prov.baseline("FPT", date(2026, 10, 23))
        assert bl is not None and bl.median_trade_value == 20e6
