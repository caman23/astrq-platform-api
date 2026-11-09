//! Command line: serve | simulate | backtest | dnse-check | ssi-check | ssi-universe | r2-check.

use anyhow::{bail, Context, Result};
use astrq::core::clock::{Clock, SimClock};
use astrq::core::config::{load_settings, Settings};
use astrq::core::context::{ContextProvider, ContextService, NullContextProvider};
use astrq::core::fmt;
use astrq::core::market_rules::MarketRules;
use astrq::core::offline::run_offline;
use astrq::core::platform::Platform;
use astrq::core::remote_store::R2Client;
use astrq::core::sessions::SessionCalendar;
use astrq::core::universe::Universe;
use astrq::providers::dnse::DnseProvider;
use astrq::providers::recorded_baseline::RecordedBaselineProvider;
use astrq::providers::replay::read_events;
use astrq::providers::ssi::rest::SsiRestClient;
use astrq::providers::ssi::SsiProvider;
use astrq::providers::synthetic::{SyntheticContextProvider, SyntheticProvider};
use astrq::providers::MarketDataProvider;
use astrq::tools::build_tool;
use chrono::{NaiveDate, Timelike};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "astrq",
    version,
    about = "Stock Tools Platform (read-only) — Radar Lệnh Lớn"
)]
struct Cli {
    /// settings YAML (default: config/settings.yaml or $ASTRQ_CONFIG)
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run API + realtime pipeline
    Serve {
        #[arg(long)]
        host: Option<String>,
        #[arg(long)]
        port: Option<u16>,
        /// synthetic speed multiplier
        #[arg(long)]
        speed: Option<f64>,
        #[arg(long)]
        plain_logs: bool,
    },
    /// Write a synthetic trading day to JSONL (for replay)
    Simulate {
        #[arg(long, default_value = "data/ticks/replay.jsonl")]
        out: PathBuf,
        #[arg(long)]
        seed: Option<u64>,
        /// YYYY-MM-DD (default: today)
        #[arg(long)]
        date: Option<NaiveDate>,
    },
    /// Run the radar on recorded events and report counts
    Backtest {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, value_parser = ["synthetic", "recorded", "none"])]
        baseline: Option<String>,
        #[arg(long)]
        no_baseline: bool,
        #[arg(short, long)]
        verbose: bool,
    },
    /// Test DNSE credentials and stream
    DnseCheck {
        #[arg(long, default_value_t = 20)]
        listen: u64,
        #[arg(long, default_value_t = 10)]
        show: usize,
        /// comma list, default: universe file
        #[arg(long)]
        symbols: Option<String>,
    },
    /// Test SSI credentials, VN100 lookup and stream
    SsiCheck {
        #[arg(long, default_value = "VN100")]
        index: String,
        #[arg(long, default_value_t = 20)]
        listen: u64,
        #[arg(long, default_value_t = 10)]
        show: usize,
    },
    /// Write official index constituents from SSI
    SsiUniverse {
        #[arg(long, default_value = "VN100")]
        index: String,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        dry_run: bool,
    },
    /// Test Cloudflare R2 credentials (write + read back)
    R2Check,
}

fn setup_logging(level: &str, json_logs: bool) {
    let filter = tracing_subscriber::EnvFilter::try_new(format!(
        "{},hyper=warn,reqwest=warn,tungstenite=warn",
        level.to_ascii_lowercase()
    ))
    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // logs go to stderr so command output (JSON reports) stays clean on stdout
    let b = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_writer(std::io::stderr);
    if json_logs {
        b.json().init();
    } else {
        b.init();
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("{e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let cfg = cli.config.as_deref();
    if !matches!(cli.cmd, Cmd::Serve { .. }) {
        setup_logging(
            &std::env::var("ASTRQ_LOG_LEVEL").unwrap_or_else(|_| "warn".into()),
            false,
        );
    }
    match cli.cmd {
        Cmd::Serve {
            host,
            port,
            speed,
            plain_logs,
        } => serve(load_settings(cfg, true)?, host, port, speed, plain_logs).await,
        Cmd::Simulate { out, seed, date } => {
            simulate(load_settings(cfg, true)?, out, seed, date).await
        }
        Cmd::Backtest {
            input,
            baseline,
            no_baseline,
            verbose,
        } => {
            backtest(
                load_settings(cfg, true)?,
                input,
                baseline,
                no_baseline,
                verbose,
            )
            .await
        }
        Cmd::DnseCheck {
            listen,
            show,
            symbols,
        } => dnse_check(load_settings(cfg, true)?, listen, show, symbols).await,
        Cmd::SsiCheck {
            index,
            listen,
            show,
        } => ssi_check(load_settings(cfg, true)?, index, listen, show).await,
        Cmd::SsiUniverse {
            index,
            out,
            dry_run,
        } => ssi_universe(load_settings(cfg, true)?, index, out, dry_run).await,
        Cmd::R2Check => r2_check(load_settings(cfg, true)?).await,
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
}

async fn serve(
    mut s: Settings,
    host: Option<String>,
    port: Option<u16>,
    speed: Option<f64>,
    plain: bool,
) -> Result<()> {
    if let Some(sp) = speed {
        s.provider.synthetic.speed = sp;
    }
    setup_logging(&s.log_level, !plain);
    // PaaS hosts (Render, …) assign the listening port via $PORT
    let port = port
        .or_else(|| std::env::var("PORT").ok().and_then(|p| p.parse().ok()))
        .unwrap_or(s.api.port);
    let host = host.unwrap_or_else(|| s.api.host.clone());
    let platform = Platform::new(s, false)?;
    platform.start().await?;
    let app = astrq::api::router(platform.clone());
    let listener = tokio::net::TcpListener::bind((host.as_str(), port))
        .await
        .with_context(|| format!("binding {host}:{port}"))?;
    tracing::info!("listening on http://{host}:{port}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    tracing::info!("shutting down");
    platform.stop().await;
    Ok(())
}

async fn simulate(
    s: Settings,
    out: PathBuf,
    seed: Option<u64>,
    date: Option<NaiveDate>,
) -> Result<()> {
    let mut cfg = s.provider.synthetic.clone();
    cfg.speed = 0.0;
    if let Some(sd) = seed {
        cfg.seed = sd;
    }
    cfg.trading_date = date.or(cfg.trading_date);
    cfg.start_time = s
        .sessions
        .schedule
        .first()
        .map(|w| w.start)
        .unwrap_or(cfg.start_time);
    let calendar = Arc::new(SessionCalendar::new(&s.sessions));
    let rules = Arc::new(MarketRules::new(&s.market_rules));
    let universe = Universe::load(&s.path(&s.universe.file))?;
    let clock: Arc<dyn Clock> = Arc::new(SimClock::new(
        SyntheticProvider::start_time(&cfg, &calendar),
        0.0,
    ));
    let mut p = SyntheticProvider::new(cfg, &universe.symbols(), calendar, rules, clock, true);
    p.connect().await?;
    if let Some(dir) = out.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut w = std::io::BufWriter::new(std::fs::File::create(&out)?);
    let mut n = 0u64;
    while let Some(ev) = p.next_event().await? {
        serde_json::to_writer(&mut w, &ev)?;
        w.write_all(b"\n")?;
        n += 1;
    }
    w.flush()?;
    println!("wrote {n} events to {}", out.display());
    Ok(())
}

async fn backtest(
    s: Settings,
    input: PathBuf,
    baseline: Option<String>,
    no_baseline: bool,
    verbose: bool,
) -> Result<()> {
    let universe = Universe::load(&s.path(&s.universe.file))?;
    let rules = MarketRules::new(&s.market_rules);
    let kind = if no_baseline {
        "none".to_string()
    } else {
        baseline.unwrap_or_else(|| s.context.kind.clone())
    };
    let provider: Arc<dyn ContextProvider> = match kind.as_str() {
        "synthetic" => Arc::new(SyntheticContextProvider {
            seed: s.provider.synthetic.seed,
            rules,
        }),
        "recorded" => {
            let data = s.path(&s.storage.data_dir);
            Arc::new(RecordedBaselineProvider::new(
                data.join("ticks"),
                data.join("baselines"),
                s.context.lookback_days,
                s.context.min_days,
            ))
        }
        _ => Arc::new(NullContextProvider),
    };
    let events = read_events(&input)?;
    let n_events = events.len();
    let tools = s
        .tools
        .iter()
        .filter(|(_, e)| e.enabled)
        .map(|(id, e)| build_tool(id, e).map(|t| (t, e.clone())))
        .collect::<Result<Vec<_>>>()?;
    let started = std::time::Instant::now();
    let res = run_offline(
        events,
        &s,
        universe,
        Arc::new(ContextService::new(provider)),
        tools,
        true,
        true,
    )
    .await?;
    let elapsed = started.elapsed().as_secs_f64();
    let calendar = SessionCalendar::new(&s.sessions);
    let sigs: Vec<_> = res
        .signals
        .iter()
        .filter(|x| x.tool_id == "large_order_radar")
        .collect();
    let mut by_kind = BTreeMap::<String, u64>::new();
    let mut by_sev = BTreeMap::<String, u64>::new();
    let mut by_hour = BTreeMap::<String, u64>::new();
    let mut by_sym = BTreeMap::<String, u64>::new();
    let mut buckets = BTreeMap::<i64, u64>::new();
    let mut flags = BTreeMap::<String, u64>::new();
    for x in &sigs {
        *by_kind.entry(x.kind.clone()).or_default() += 1;
        *by_sev.entry(x.severity.as_str().into()).or_default() += 1;
        *by_hour
            .entry(format!("{:02}:00", calendar.local(x.ts).hour()))
            .or_default() += 1;
        if x.kind == "block_order_open" {
            *by_sym.entry(x.symbol.clone()).or_default() += 1;
        }
        *buckets
            .entry((x.score / 10.0).floor() as i64 * 10)
            .or_default() += 1;
        for f in &x.quality_flags {
            *flags.entry(f.clone()).or_default() += 1;
        }
    }
    let mut top: Vec<(String, u64)> = by_sym.into_iter().collect();
    top.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    top.truncate(10);
    // accuracy table (price 5/15/30 minutes after each order's first signal)
    let follow_through = res
        .runners
        .get("large_order_radar")
        .and_then(|r| {
            let all: Vec<_> = res.signals.iter().cloned().map(Arc::new).collect();
            r.tool.lock().view(&all, &r.ctx, &Default::default())
        })
        .map(|v| v["stats"].clone())
        .unwrap_or_default();
    let report = json!({
        "input": input.display().to_string(),
        "events": n_events,
        "ingest": res.ingest_stats,
        "signals": sigs.len(),
        "by_kind": by_kind,
        "by_severity": by_sev,
        "by_hour_vn": by_hour,
        "top_symbols_open": top,
        "score_histogram": buckets.iter().map(|(k, v)| (format!("{k}-{}", k + 9), *v)).collect::<BTreeMap<_, _>>(),
        "quality_flags": flags,
        "elapsed_sec": (elapsed * 1000.0).round() / 1000.0,
        "events_per_sec": (n_events as f64 / elapsed.max(1e-9)).round(),
        "follow_through": follow_through,
        "note": "Đếm tín hiệu để chỉnh ngưỡng. follow_through: tỷ lệ giá đi đúng chiều lệnh sau 5/15/30 phút trên dữ liệu này — thống kê quá khứ, không phải dự báo.",
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    if verbose {
        for x in sigs.iter().filter(|x| x.severity.as_str() != "info") {
            let v = x
                .features
                .get("group")
                .and_then(|g| g["total_value"].as_f64())
                .unwrap_or(0.0);
            println!(
                "{} {:5} {:20} {:7} {:5.1} {:6} {}",
                fmt::hms(x.ts),
                x.symbol,
                x.kind,
                x.side.as_str(),
                x.score,
                x.severity.as_str(),
                fmt::vnd(v)
            );
        }
    }
    Ok(())
}

async fn listen_sample(provider: &mut dyn MarketDataProvider, secs: u64, show: usize) {
    let mut shown = 0;
    let _ = tokio::time::timeout(Duration::from_secs(secs), async {
        while let Ok(Some(ev)) = provider.next_event().await {
            if shown < show {
                let mut v = serde_json::to_value(&ev).unwrap_or_default();
                if let Some(o) = v.as_object_mut() {
                    o.remove("provider");
                    o.remove("ts_received");
                }
                println!("   {v}");
                shown += 1;
            }
        }
    })
    .await;
}

async fn dnse_check(s: Settings, listen: u64, show: usize, symbols: Option<String>) -> Result<()> {
    let cfg = s.provider.dnse.clone();
    if cfg.api_key.is_none() || cfg.api_secret.is_none() {
        bail!("Thiếu ASTRQ_PROVIDER__DNSE__API_KEY / API_SECRET trong .env");
    }
    let mut p = DnseProvider::new(
        cfg.clone(),
        Arc::new(SessionCalendar::new(&s.sessions)),
        None,
    );
    p.connect().await?;
    println!("✔ Xác thực WebSocket DNSE thành công");
    let symbols: Vec<String> = match symbols {
        Some(list) => list
            .split(',')
            .map(|x| x.trim().to_ascii_uppercase())
            .collect(),
        None => Universe::load(&s.path(&s.universe.file))?.symbols(),
    };
    p.subscribe(&symbols).await?;
    println!(
        "✔ Đăng ký {} cho {} mã · nghe {listen}s",
        cfg.channels.join(", "),
        symbols.len()
    );
    listen_sample(&mut p, listen, show).await;
    let stats = p.status().lock().stats.clone();
    p.close().await;
    let out = if stats.as_object().is_some_and(|o| !o.is_empty()) {
        stats
    } else {
        p.normalizer.summary()
    };
    println!("{}", serde_json::to_string_pretty(&out)?);
    println!(
        "Kiểm tra: giá đã đúng đơn vị đồng chưa (so với bảng giá)? sides_seen/sessions_seen là các mã cần xác nhận; message_shapes cho biết dạng bản tin thực tế."
    );
    Ok(())
}

async fn ssi_check(s: Settings, index: String, listen: u64, show: usize) -> Result<()> {
    let cfg = s.provider.ssi.clone();
    if cfg.consumer_id.is_none() || cfg.consumer_secret.is_none() {
        bail!("Thiếu ASTRQ_PROVIDER__SSI__CONSUMER_ID / CONSUMER_SECRET trong .env");
    }
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut rest = SsiRestClient::new(cfg.clone(), http.clone());
    rest.token().await?;
    println!("✔ Lấy access token thành công");
    let (exchange, symbols) = rest.index_components(&index).await?;
    println!(
        "✔ {index}: {} mã ({}) — vd: {}",
        symbols.len(),
        exchange.unwrap_or_default(),
        symbols
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    );
    if listen == 0 {
        return Ok(());
    }
    let mut p = SsiProvider::new(
        cfg,
        Arc::new(SessionCalendar::new(&s.sessions)),
        None,
        Some(http),
    );
    p.connect().await?;
    let syms = if symbols.is_empty() {
        Universe::load(&s.path(&s.universe.file))?.symbols()
    } else {
        symbols
    };
    p.subscribe(&syms).await?;
    println!(
        "✔ Đã kết nối streaming, kênh {}…  nghe {listen}s",
        p.channel_string().chars().take(60).collect::<String>()
    );
    listen_sample(&mut p, listen, show).await;
    p.close().await;
    println!("{}", serde_json::to_string_pretty(&p.normalizer.summary())?);
    println!("Kiểm tra: giá có đúng đơn vị đồng không? sessions_seen là các mã TradingSession cần điền vào provider.ssi.session_map.");
    Ok(())
}

async fn ssi_universe(
    s: Settings,
    index: String,
    out: Option<PathBuf>,
    dry_run: bool,
) -> Result<()> {
    let path = out.unwrap_or_else(|| s.path(&s.universe.file));
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let (exchange, symbols) = SsiRestClient::new(s.provider.ssi.clone(), http)
        .index_components(&index)
        .await?;
    if symbols.is_empty() {
        bail!("SSI không trả về thành phần cho {index}; giữ nguyên file cũ");
    }
    let sectors: BTreeMap<String, String> = Universe::load(&path)
        .map(|u| {
            u.instruments()
                .into_iter()
                .filter_map(|i| i.sector.map(|s| (i.symbol, s)))
                .collect()
        })
        .unwrap_or_default();
    let mut syms = symbols.clone();
    syms.sort();
    let calendar = SessionCalendar::new(&s.sessions);
    let doc = json!({
        "name": index,
        "as_of": chrono::Utc::now().with_timezone(&calendar.tz).date_naive().to_string(),
        "source": "SSI FastConnect Data IndexComponents",
        "verified": true,
        "instruments": syms.iter().map(|x| json!({
            "symbol": x, "exchange": exchange.clone().unwrap_or_else(|| "HOSE".into()),
            "sector": sectors.get(x).cloned().unwrap_or_else(|| "Khác".into()),
        })).collect::<Vec<_>>(),
    });
    let yaml = serde_yaml::to_string(&doc)?;
    if dry_run {
        println!("{yaml}");
        return Ok(());
    }
    let header = format!(
        "# Generated by `astrq ssi-universe --index {index}`. Re-run after each index\n# rebalancing; the running server reloads this file automatically.\n"
    );
    std::fs::write(&path, header + &yaml)?;
    let missing: Vec<&String> = syms.iter().filter(|x| !sectors.contains_key(*x)).collect();
    println!("✔ Ghi {} mã vào {}", syms.len(), path.display());
    if !missing.is_empty() {
        println!(
            "  {} mã chưa có ngành (để 'Khác'): {}",
            missing.len(),
            missing
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(())
}

async fn r2_check(s: Settings) -> Result<()> {
    let cfg = s.storage.remote.clone();
    if cfg.kind != "r2" {
        bail!("Chưa bật R2: thêm ASTRQ_STORAGE__REMOTE__KIND=r2 và 4 biến ENDPOINT, BUCKET, ACCESS_KEY_ID, SECRET_ACCESS_KEY vào .env");
    }
    let client = R2Client::new(cfg.clone(), None);
    let key = "healthcheck/r2-check.txt";
    let body = format!("astrq r2-check {}", chrono::Utc::now().to_rfc3339()).into_bytes();
    let hint = |e: &anyhow::Error| {
        let m = e.to_string();
        if m.contains("403") {
            " → sai Access Key/Secret, hoặc token không có quyền Object Read & Write"
        } else if m.contains("404") {
            " → sai tên bucket hoặc sai Endpoint (account id)"
        } else {
            ""
        }
    };
    if let Err(e) = client.put(key, body.clone(), "text/plain").await {
        bail!("✖ {e}{}", hint(&e));
    }
    println!(
        "✔ Ghi được vào bucket '{}' ({}{key})",
        cfg.bucket.unwrap_or_default(),
        cfg.prefix
    );
    match client.get(key).await {
        Ok(Some(got)) if got == body => {
            println!("✔ Đọc lại khớp — thông tin R2 đúng, dùng được cho Render")
        }
        Ok(_) => bail!("✖ Đọc lại không khớp nội dung vừa ghi"),
        Err(e) => bail!("✖ {e}{}", hint(&e)),
    }
    Ok(())
}
