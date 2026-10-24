"""Remote persistence on S3-compatible storage (Cloudflare R2) for disk-less hosts.

What is kept (all small):
  * {prefix}baselines/summaries.json.gz — per-day, per-symbol trade-size summaries
    (the input of RecordedBaselineProvider), last `keep_summary_days` days;
  * {prefix}signals/YYYY-MM-DD.jsonl.gz — today's signals, re-uploaded periodically.
Raw ticks are NOT uploaded (too large for free egress); a restart mid-session loses the
ticks before it, so that day's baseline summary is partial.

Requests are signed with AWS Signature V4 (R2 uses region "auto", path-style URLs).
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import hmac
import json
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from astrq.core.config import RemoteStorageConfig
from astrq.providers.recorded_baseline import summarize_day

log = logging.getLogger(__name__)

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def sign_v4(
    method: str,
    url: httpx.URL,
    headers: dict[str, str],
    payload_hash: str,
    access_key: str,
    secret_key: str,
    region: str,
    now: datetime,
    service: str = "s3",
) -> dict[str, str]:
    """Return headers including Authorization for an AWS SigV4 request."""
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    all_headers = {
        **headers,
        "host": url.netloc.decode(),
        "x-amz-date": amz_date,
        "x-amz-content-sha256": payload_hash,
    }
    canon = {k.lower().strip(): " ".join(str(v).split()) for k, v in all_headers.items()}
    signed = ";".join(sorted(canon))
    query = "&".join(
        f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}"
        for k, v in sorted(url.params.multi_items())
    )
    canonical_request = "\n".join(
        [
            method,
            quote(url.path, safe="/-_.~"),
            query,
            "".join(f"{k}:{canon[k]}\n" for k in sorted(canon)),
            signed,
            payload_hash,
        ]
    )
    scope = f"{day}/{region}/{service}/aws4_request"
    to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    k = _hmac(
        _hmac(_hmac(_hmac(f"AWS4{secret_key}".encode(), day), region), service), "aws4_request"
    )
    signature = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    out = {**headers, "x-amz-date": amz_date, "x-amz-content-sha256": payload_hash}
    out["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed}, "
        f"Signature={signature}"
    )
    return out


class R2Client:
    def __init__(self, cfg: RemoteStorageConfig, http: httpx.AsyncClient | None = None) -> None:
        assert cfg.endpoint and cfg.bucket and cfg.access_key_id and cfg.secret_access_key
        self.cfg = cfg
        self._http = http or httpx.AsyncClient(timeout=30.0)
        self._own = http is None

    def _url(self, key: str) -> httpx.URL:
        assert self.cfg.endpoint and self.cfg.bucket
        return httpx.URL(
            f"{self.cfg.endpoint.rstrip('/')}/{self.cfg.bucket}/{self.cfg.prefix}{key}"
        )

    def _sign(
        self, method: str, url: httpx.URL, body: bytes, headers: dict[str, str]
    ) -> dict[str, str]:
        assert self.cfg.access_key_id and self.cfg.secret_access_key
        return sign_v4(
            method,
            url,
            headers,
            hashlib.sha256(body).hexdigest() if body else EMPTY_SHA256,
            self.cfg.access_key_id,
            self.cfg.secret_access_key.get_secret_value(),
            self.cfg.region,
            datetime.now(UTC),
        )

    async def put(
        self, key: str, body: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        url = self._url(key)
        r = await self._http.put(
            url, content=body, headers=self._sign("PUT", url, body, {"content-type": content_type})
        )
        if r.status_code >= 300:
            raise RuntimeError(f"R2 PUT {key}: HTTP {r.status_code}")

    async def get(self, key: str) -> bytes | None:
        url = self._url(key)
        r = await self._http.get(url, headers=self._sign("GET", url, b"", {}))
        if r.status_code == 404:
            return None
        if r.status_code >= 300:
            raise RuntimeError(f"R2 GET {key}: HTTP {r.status_code}")
        return r.content

    async def close(self) -> None:
        if self._own:
            await self._http.aclose()


class RemoteSync:
    """Restore/persist the small state a disk-less host needs between restarts."""

    def __init__(self, client: R2Client, data_dir: Path, keep_days: int = 30) -> None:
        self.client = client
        self.data_dir = data_dir
        self.keep_days = keep_days
        self.last_error: str | None = None
        self.last_upload: datetime | None = None

    @property
    def baselines_dir(self) -> Path:
        return self.data_dir / "baselines"

    def _signals_file(self, d: date) -> Path:
        return self.data_dir / "signals" / f"{d.isoformat()}.jsonl"

    async def restore(self, today: date) -> None:
        try:
            blob = await self.client.get("baselines/summaries.json.gz")
            if blob:
                summaries: dict[str, Any] = json.loads(gzip.decompress(blob))
                self.baselines_dir.mkdir(parents=True, exist_ok=True)
                for day, summary in summaries.items():
                    f = self.baselines_dir / f"{day}.json"
                    if not f.exists():
                        f.write_text(json.dumps(summary))
                log.info("restored %d baseline summaries from R2", len(summaries))
            sig = await self.client.get(f"signals/{today.isoformat()}.jsonl.gz")
            local = self._signals_file(today)
            if sig and not local.exists():
                local.parent.mkdir(parents=True, exist_ok=True)
                local.write_bytes(gzip.decompress(sig))
                log.info("restored today's signals from R2")
        except Exception as exc:  # storage is best-effort; never block startup
            self.last_error = f"restore: {exc}"
            log.warning("R2 restore failed: %s", exc)

    async def upload_signals(self, today: date) -> None:
        f = self._signals_file(today)
        if not f.exists():
            return
        try:
            body = await asyncio.to_thread(lambda: gzip.compress(f.read_bytes()))
            await self.client.put(f"signals/{today.isoformat()}.jsonl.gz", body, "application/gzip")
            self.last_upload = datetime.now(UTC)
        except Exception as exc:
            self.last_error = f"signals: {exc}"
            log.warning("R2 signals upload failed: %s", exc)

    async def upload_day_summary(self, today: date) -> None:
        """Summarize today's ticks and merge into the remote summaries object."""
        ticks = self.data_dir / "ticks" / f"{today.isoformat()}.jsonl"
        try:
            summaries: dict[str, Any] = {}
            blob = await self.client.get("baselines/summaries.json.gz")
            if blob:
                summaries = json.loads(gzip.decompress(blob))
            if ticks.exists():
                summary = await asyncio.to_thread(summarize_day, ticks)
                if summary:
                    summaries[today.isoformat()] = summary
                    self.baselines_dir.mkdir(parents=True, exist_ok=True)
                    (self.baselines_dir / f"{today.isoformat()}.json").write_text(
                        json.dumps(summary)
                    )
            keep = dict(sorted(summaries.items())[-self.keep_days :])
            await self.client.put(
                "baselines/summaries.json.gz",
                gzip.compress(json.dumps(keep).encode()),
                "application/gzip",
            )
            log.info("uploaded baseline summaries (%d days) to R2", len(keep))
        except Exception as exc:
            self.last_error = f"summary: {exc}"
            log.warning("R2 summary upload failed: %s", exc)
