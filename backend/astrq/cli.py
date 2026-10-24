"""Command line: serve | simulate | backtest | ssi-check | ssi-universe."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import Counter
from datetime import date
from pathlib import Path

from pydantic import ValidationError

from astrq.core import fmt
from astrq.core.clock import SimClock
from astrq.core.config import load_settings
from astrq.core.context import ContextProvider, ContextService, NullContextProvider
from astrq.core.logging import setup_logging
from astrq.core.market_rules import MarketRules
from astrq.core.offline import run_offline
from astrq.core.sessions import SessionCalendar
from astrq.core.universe import Universe
from astrq.providers.recorded_baseline import RecordedBaselineProvider
from astrq.providers.replay import read_events
from astrq.providers.synthetic import SyntheticContextProvider, SyntheticProvider


def _serve(args: argparse.Namespace) -> None:
    import uvicorn

    from astrq.api.app import create_app

    settings = load_settings(args.config)
    if args.speed is not None:
        settings.provider.synthetic.speed = args.speed
    setup_logging(settings.log_level.upper(), json_logs=not args.plain_logs)
    app = create_app(settings)
    uvicorn.run(
        app,
        host=args.host or settings.api.host,
        # PaaS hosts (Render, …) assign the listening port via $PORT
        port=args.port or int(os.environ.get("PORT") or settings.api.port),
        log_config=None,
    )


async def _simulate(args: argparse.Namespace) -> None:
    settings = load_settings(args.config)
    cfg = settings.provider.synthetic.model_copy(
        update={
            "speed": 0.0,
            "seed": args.seed if args.seed is not None else settings.provider.synthetic.seed,
            "trading_date": date.fromisoformat(args.date) if args.date else None,
            "start_time": settings.sessions.schedule[0].start,
        }
    )
    calendar = SessionCalendar(settings.sessions)
    rules = MarketRules(settings.market_rules)
    universe = Universe(settings.path(settings.universe.file))
    clock = SimClock(SyntheticProvider.start_time(cfg, calendar), 0.0)
    prov = SyntheticProvider(cfg, universe.symbols, calendar, rules, clock, stop_at_close=True)
    await prov.connect()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with out.open("w", encoding="utf-8") as fh:
        async for ev in prov.events():
            fh.write(ev.model_dump_json() + "\n")
            n += 1
    print(f"wrote {n} events to {out}")


async def _backtest(args: argparse.Namespace) -> None:
    settings = load_settings(args.config)
    universe = Universe(settings.path(settings.universe.file))
    rules = MarketRules(settings.market_rules)
    kind = "none" if args.no_baseline else (args.baseline or settings.context.kind)
    provider: ContextProvider
    if kind == "synthetic":
        provider = SyntheticContextProvider(settings.provider.synthetic.seed, rules)
    elif kind == "recorded":
        data = settings.path(settings.storage.data_dir)
        provider = RecordedBaselineProvider(
            data / "ticks",
            data / "baselines",
            settings.context.lookback_days,
            settings.context.min_days,
        )
    else:
        provider = NullContextProvider()
    ctx = ContextService(provider)
    events = read_events(Path(args.input))
    res = await run_offline(events, settings, universe, ctx)
    sigs = [s for s in res.signals if s.tool_id == "large_order_radar"]
    calendar = SessionCalendar(settings.sessions)
    by_kind = Counter(s.kind for s in sigs)
    by_sev = Counter(s.severity.value for s in sigs)
    by_hour = Counter(calendar.local(s.ts).strftime("%H:00") for s in sigs)
    by_sym = Counter(s.symbol for s in sigs if s.kind == "block_order_open")
    buckets = Counter(int(s.score // 10) * 10 for s in sigs)
    flags = Counter(f for s in sigs for f in s.quality_flags)
    report = {
        "input": args.input,
        "events": len(events),
        "ingest": res.ingest_stats,
        "signals": len(sigs),
        "by_kind": dict(by_kind),
        "by_severity": dict(by_sev),
        "by_hour_vn": dict(sorted(by_hour.items())),
        "top_symbols_open": by_sym.most_common(10),
        "score_histogram": {f"{k}-{k + 9}": v for k, v in sorted(buckets.items())},
        "quality_flags": dict(flags),
        "note": "Báo cáo đếm tín hiệu để chỉnh ngưỡng; không đánh giá hiệu quả dự báo giá.",
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.verbose:
        for s in sigs:
            if s.severity.value != "info":
                print(
                    f"{fmt.hms(s.ts)} {s.symbol:5} {s.kind:20} {s.side.value:7} "
                    f"{s.score:5.1f} {s.severity.value:6} "
                    f"{fmt.vnd(s.features.get('group', {}).get('total_value', 0))}"
                )


async def _ssi_check(args: argparse.Namespace) -> None:
    """Verify SSI credentials, VN100 lookup and (optionally) a short stream sample."""
    import httpx

    from astrq.providers.ssi import SsiProvider
    from astrq.providers.ssi.rest import SsiRestClient

    settings = load_settings(args.config)
    cfg = settings.provider.ssi
    if not cfg.consumer_id or cfg.consumer_secret is None:
        raise SystemExit("Thiếu ASTRQ_PROVIDER__SSI__CONSUMER_ID / CONSUMER_SECRET trong .env")
    async with httpx.AsyncClient(timeout=20.0) as http:
        rest = SsiRestClient(cfg, http)
        await rest.token()
        print("✔ Lấy access token thành công")
        exchange, symbols = await rest.index_components(args.index)
        print(f"✔ {args.index}: {len(symbols)} mã ({exchange}) — vd: {', '.join(symbols[:8])}")
        if args.listen <= 0:
            return
        calendar = SessionCalendar(settings.sessions)
        prov = SsiProvider(cfg, calendar, http=http)
        await prov.connect()
        await prov.subscribe(symbols or Universe(settings.path(settings.universe.file)).symbols, [])
        print(f"✔ Đã kết nối streaming, kênh {prov.channel_string()[:60]}…  nghe {args.listen}s")
        shown = 0

        async def consume() -> None:
            nonlocal shown
            async for ev in prov.events():
                if shown < args.show:
                    print("  ", ev.model_dump_json(exclude={"provider", "ts_received"}))
                    shown += 1

        try:
            await asyncio.wait_for(consume(), args.listen)
        except TimeoutError:
            pass
        finally:
            await prov.close()
        print(json.dumps(prov.normalizer.summary(), ensure_ascii=False, indent=2))
        print(
            "Kiểm tra: giá có đúng đơn vị đồng không? sessions_seen là các mã TradingSession "
            "cần điền vào provider.ssi.session_map."
        )


async def _dnse_check(args: argparse.Namespace) -> None:
    """Verify DNSE credentials (WebSocket auth) and print a short stream sample."""
    from astrq.providers.dnse import DnseProvider

    settings = load_settings(args.config)
    cfg = settings.provider.dnse
    if not cfg.api_key or cfg.api_secret is None:
        raise SystemExit("Thiếu ASTRQ_PROVIDER__DNSE__API_KEY / API_SECRET trong .env")
    prov = DnseProvider(cfg, SessionCalendar(settings.sessions))
    await prov.connect()
    print("✔ Xác thực WebSocket DNSE thành công")
    symbols = (
        args.symbols.split(",")
        if args.symbols
        else Universe(settings.path(settings.universe.file)).symbols
    )
    await prov.subscribe(symbols, [])
    print(f"✔ Đăng ký {', '.join(cfg.channels)} cho {len(symbols)} mã · nghe {args.listen}s")
    shown = 0

    async def consume() -> None:
        nonlocal shown
        async for ev in prov.events():
            if shown < args.show:
                print("  ", ev.model_dump_json(exclude={"provider", "ts_received"}))
                shown += 1

    try:
        await asyncio.wait_for(consume(), args.listen)
    except TimeoutError:
        pass
    finally:
        await prov.close()
    print(json.dumps(prov.status.stats or prov.normalizer.summary(), ensure_ascii=False, indent=2))
    print(
        "Kiểm tra: giá đã đúng đơn vị đồng chưa (so với bảng giá)? sides_seen/sessions_seen "
        "là các mã cần xác nhận; message_shapes cho biết dạng bản tin thực tế."
    )


async def _ssi_universe(args: argparse.Namespace) -> None:
    """Write the official index constituents (SSI IndexComponents) to the universe file."""
    from datetime import datetime

    import httpx
    import yaml

    from astrq.providers.ssi.rest import SsiRestClient

    settings = load_settings(args.config)
    path = settings.path(args.out or settings.universe.file)
    async with httpx.AsyncClient(timeout=20.0) as http:
        exchange, symbols = await SsiRestClient(settings.provider.ssi, http).index_components(
            args.index
        )
    if not symbols:
        raise SystemExit(f"SSI không trả về thành phần cho {args.index}; giữ nguyên file cũ")
    sectors: dict[str, str | None] = {}
    if path.exists():  # keep sector labels we already have
        old = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        sectors = {i["symbol"]: i.get("sector") for i in old.get("instruments", [])}
    doc = {
        "name": args.index,
        "as_of": datetime.now(SessionCalendar(settings.sessions).tz).date().isoformat(),
        "source": "SSI FastConnect Data IndexComponents",
        "verified": True,
        "instruments": [
            {"symbol": s, "exchange": exchange or "HOSE", "sector": sectors.get(s) or "Khác"}
            for s in sorted(symbols)
        ],
    }
    if args.dry_run:
        print(yaml.safe_dump(doc, allow_unicode=True, sort_keys=False))
        return
    header = (
        f"# Generated by `astrq ssi-universe --index {args.index}`. Re-run after each index\n"
        "# rebalancing; the running server reloads this file automatically.\n"
    )
    path.write_text(header + yaml.safe_dump(doc, allow_unicode=True, sort_keys=False), "utf-8")
    missing = [s for s in symbols if not sectors.get(s)]
    print(f"✔ Ghi {len(symbols)} mã vào {path}")
    if missing:
        print(f"  {len(missing)} mã chưa có ngành (để 'Khác'): {', '.join(missing)}")


def main() -> None:
    p = argparse.ArgumentParser(prog="astrq")
    p.add_argument("--config", default=None, help="settings YAML (default: config/settings.yaml)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="run API + realtime pipeline")
    s.add_argument("--host")
    s.add_argument("--port", type=int)
    s.add_argument("--speed", type=float, help="synthetic speed multiplier")
    s.add_argument("--plain-logs", action="store_true")

    m = sub.add_parser("simulate", help="write a synthetic trading day to JSONL (for replay)")
    m.add_argument("--out", default="data/ticks/replay.jsonl")
    m.add_argument("--seed", type=int)
    m.add_argument("--date", help="YYYY-MM-DD (default: today)")

    b = sub.add_parser("backtest", help="run the radar on recorded events and report counts")
    b.add_argument("--input", required=True)
    b.add_argument("--baseline", choices=["synthetic", "recorded", "none"])
    b.add_argument("--no-baseline", action="store_true")
    b.add_argument("-v", "--verbose", action="store_true")

    c = sub.add_parser("ssi-check", help="test SSI credentials, VN100 lookup and stream")
    c.add_argument("--index", default="VN100")
    c.add_argument("--listen", type=int, default=20, help="seconds of streaming (0 = skip)")
    c.add_argument("--show", type=int, default=10, help="sample events to print")

    dn = sub.add_parser("dnse-check", help="test DNSE credentials and stream")
    dn.add_argument("--listen", type=int, default=20, help="seconds of streaming")
    dn.add_argument("--show", type=int, default=10, help="sample events to print")
    dn.add_argument("--symbols", help="comma list, default: universe file")

    u = sub.add_parser("ssi-universe", help="write official index constituents from SSI")
    u.add_argument("--index", default="VN100")
    u.add_argument("--out", help="default: universe.file from settings")
    u.add_argument("--dry-run", action="store_true")

    args = p.parse_args()
    setup_logging(os.environ.get("ASTRQ_LOG_LEVEL", "WARNING"), json_logs=False)
    commands = {
        "simulate": _simulate,
        "backtest": _backtest,
        "ssi-check": _ssi_check,
        "dnse-check": _dnse_check,
        "ssi-universe": _ssi_universe,
    }
    try:
        if args.cmd == "serve":
            _serve(args)
        else:
            asyncio.run(commands[args.cmd](args))
    except ValidationError as exc:
        # print messages only: pydantic's default text echoes input values (could be secrets)
        msgs = "\n".join(f"  - {e['msg']}" for e in exc.errors())
        raise SystemExit(f"Cấu hình không hợp lệ:\n{msgs}") from None


if __name__ == "__main__":
    main()
