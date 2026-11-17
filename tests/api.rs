//! API end-to-end: real platform on a synthetic market, real HTTP server + WebSocket.

use astrq::core::config::{load_settings, Secret, Settings};
use astrq::core::platform::Platform;
use chrono::Timelike;
use futures_util::StreamExt;
use serde_json::Value;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn settings(tmp: &std::path::Path) -> Settings {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    // ignore the developer's personal .env (it may point at a real feed)
    let mut s = load_settings(Some(&root.join("config/settings.yaml")), false).unwrap();
    s.provider.kind = "synthetic".into();
    s.context.kind = "synthetic".into();
    s.storage.remote.kind = "none".into();
    s.sessions.enforce_trading_days = false;
    s.provider.synthetic.speed = 400.0; // ~1 trading minute per 0.15 s
    s.storage.data_dir = tmp.to_string_lossy().into_owned();
    s.api.frontend_dist = tmp.join("nope").to_string_lossy().into_owned();
    s
}

async fn serve(
    s: Settings,
) -> (
    String,
    std::sync::Arc<Platform>,
    tokio::task::JoinHandle<()>,
) {
    let p = Platform::new(s, false).unwrap();
    p.start().await.unwrap();
    let app = astrq::api::router(p.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    let limits = p.s.api.limits.clone();
    let h = tokio::spawn(async move {
        astrq::api::server::serve(l, app, &limits, std::future::pending())
            .await
            .unwrap()
    });
    (base, p, h)
}

#[tokio::test(flavor = "multi_thread")]
async fn api_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let (base, platform, server) = serve(settings(tmp.path())).await;
    let c = reqwest::Client::new();
    let get = |path: &str| c.get(format!("{base}{path}")).send();

    let r = get("/health").await.unwrap();
    let csp = r.headers()["content-security-policy"]
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        csp.contains("frame-ancestors 'self'")
            && csp.contains("https://*.astrq.io.vn")
            && csp.contains("script-src 'self'")
            && csp.contains("object-src 'none'"),
        "{csp}"
    );
    assert_eq!(r.headers()["x-content-type-options"], "nosniff");
    assert!(r.headers().contains_key("referrer-policy"));
    assert_eq!(r.json::<Value>().await.unwrap()["status"], "ok");

    let deadline = Instant::now() + Duration::from_secs(25);
    let mut view = Value::Null;
    while Instant::now() < deadline {
        view = get("/api/tools/large_order_radar/view")
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if view["symbols"].as_array().is_some_and(|a| a.len() >= 3) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let syms = view["symbols"].as_array().expect("symbols");
    assert!(syms.len() >= 3, "radar should light up on synthetic data");
    let with_trades = syms
        .iter()
        .flat_map(|s| s["orders"].as_array().unwrap().iter())
        .find(|o| !o["trades"].as_array().unwrap().is_empty());
    let order = with_trades.expect("latest orders carry child trades for hover details");
    for k in ["started_at", "last_at", "total_value", "n_trades", "side"] {
        assert!(order.get(k).is_some(), "order missing {k}");
    }
    let sym = syms[0]["symbol"].as_str().unwrap();
    let detail: Value = get(&format!("/api/tools/large_order_radar/view?symbol={sym}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail["symbols"][0]["symbol"], sym);

    let st: Value = get("/api/status").await.unwrap().json().await.unwrap();
    assert_eq!(st["provider"]["connected"], true);
    assert!(st["disclaimer"].as_str().unwrap().contains("khuyến nghị"));
    assert_eq!(st["universe"]["verified"], false);

    let tools: Value = get("/api/tools").await.unwrap().json().await.unwrap();
    let ids: Vec<&str> = tools
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["manifest"]["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["large_order_radar", "tick_counter"]);
    assert!(
        tools[0]["manifest"]["config_schema"]["properties"]["abs_threshold_value"]["description"]
            .is_string()
    );
    let r = c
        .post(format!("{base}/api/tools/tick_counter/enabled"))
        .json(&serde_json::json!({"enabled": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.json::<Value>().await.unwrap()["enabled"], false);

    assert_eq!(
        get("/api/signals?min_severity=alert&limit=5")
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(get("/api/signals?limit=5000").await.unwrap().status(), 422);
    assert!(get("/metrics")
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
        .contains("astrq_ticks_total"));
    assert_eq!(get("/ready").await.unwrap().status(), 200);
    let gz = c
        .get(format!("{base}/api/tools"))
        .header("Accept-Encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(
        gz.headers()
            .get("content-encoding")
            .map(|v| v.to_str().unwrap()),
        Some("gzip")
    );

    let ws_url = base.replace("http://", "ws://") + "/ws";
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url).await.unwrap();
    let hello: Value =
        serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(hello["type"], "hello");
    let mut kinds = std::collections::HashSet::new();
    for _ in 0..5 {
        let m: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        kinds.insert(m["type"].as_str().unwrap().to_string());
    }
    assert!(kinds
        .iter()
        .any(|k| ["signal", "status", "alert"].contains(&k.as_str())));

    server.abort();
    platform.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn origin_secret_and_tool_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let mut s = settings(tmp.path());
    s.api.origin_secret = Some(Secret::new("s3cr3t-origin-value"));
    s.api.allow_tool_control = false;
    let (base, platform, server) = serve(s).await;
    let c = reqwest::Client::new();
    assert_eq!(
        c.get(format!("{base}/health"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        c.get(format!("{base}/api/status"))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        c.get(format!("{base}/api/status"))
            .header("X-Astrq-Origin", "wrong")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let ok = c
        .get(format!("{base}/api/status"))
        .header("X-Astrq-Origin", "s3cr3t-origin-value")
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
    // WebSocket upgrade without the header is rejected
    assert!(
        tokio_tungstenite::connect_async(base.replace("http://", "ws://") + "/ws")
            .await
            .is_err()
    );
    let r = c
        .post(format!("{base}/api/tools/large_order_radar/enabled"))
        .header("X-Astrq-Origin", "s3cr3t-origin-value")
        .json(&serde_json::json!({"enabled": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let tools: Value = c
        .get(format!("{base}/api/tools"))
        .header("X-Astrq-Origin", "s3cr3t-origin-value")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(tools
        .as_array()
        .unwrap()
        .iter()
        .all(|t| t["control_enabled"] == false && t["status"]["enabled"] == true));
    server.abort();
    platform.stop().await;
}

/// The radar view is rendered once per data version and shared by every viewer:
/// ETag/304 revalidation, a pre-compressed gzip body, and a compact default overview.
#[tokio::test(flavor = "multi_thread")]
async fn view_cache_and_compact_overview() {
    use std::io::Read;
    let tmp = tempfile::tempdir().unwrap();
    let (base, platform, server) = serve(settings(tmp.path())).await;
    let c = reqwest::Client::new();
    let url = format!("{base}/api/tools/large_order_radar/view");

    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let v: Value = c.get(&url).send().await.unwrap().json().await.unwrap();
        let n_orders: usize = v["symbols"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["orders"].as_array().unwrap().len())
            .sum();
        if n_orders >= 20 || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    // Freeze the data. Events already queued at stop() are still applied and move day
    // totals without a new signal (the cache's max age exists for exactly that), so
    // wait until two renderings past the max age agree.
    platform.stop().await;
    let mut last = String::new();
    loop {
        let r = c.get(&url).send().await.unwrap();
        let tag = r.headers()["etag"].to_str().unwrap().to_string();
        if tag == last {
            break;
        }
        last = tag;
        tokio::time::sleep(Duration::from_millis(2100)).await;
    }

    let r = c.get(&url).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["cache-control"], "no-cache");
    let etag = r.headers()["etag"].to_str().unwrap().to_string();
    let raw = r.bytes().await.unwrap();
    let compact: Value = serde_json::from_slice(&raw).unwrap();

    // revalidation: unchanged data costs a 304 without a body (also after VIEW_MAX_AGE)
    for wait in [0, 2100] {
        tokio::time::sleep(Duration::from_millis(wait)).await;
        let r = c
            .get(&url)
            .header("If-None-Match", &etag)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 304, "after {wait} ms");
        assert!(r.bytes().await.unwrap().is_empty());
    }

    // unknown query parameters neither split nor bypass the cache
    let r = c.get(format!("{url}?cachebust=123")).send().await.unwrap();
    assert_eq!(r.headers()["etag"].to_str().unwrap(), etag);

    // gzip: served pre-compressed, same document
    let r = c
        .get(&url)
        .header("Accept-Encoding", "gzip, deflate, br")
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["content-encoding"], "gzip");
    let gz = r.bytes().await.unwrap();
    assert!(gz.len() * 3 < raw.len(), "gzip should be much smaller");
    let mut unzipped = Vec::new();
    flate2::read::GzDecoder::new(&gz[..])
        .read_to_end(&mut unzipped)
        .unwrap();
    assert_eq!(unzipped, raw);

    // many viewers at once: all get the same rendering
    let tags = futures_util::future::join_all((0..32).map(|_| c.get(&url).send())).await;
    for r in tags {
        let r = r.unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(r.headers()["etag"].to_str().unwrap(), etag);
    }

    // compact overview = what the radar hover needs; ?full=1 = the v1 document
    let full: Value = c
        .get(format!("{url}?full=1"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let syms = |v: &Value| v["symbols"].as_array().unwrap().clone();
    assert_eq!(syms(&compact).len(), syms(&full).len());
    for (a, b) in syms(&compact).iter().zip(syms(&full).iter()) {
        assert_eq!(a["symbol"], b["symbol"]);
        assert_eq!(a["max_score"], b["max_score"]);
        assert_eq!(a["n_orders"], b["n_orders"]);
        assert_eq!(a["day_net_large"], b["day_net_large"]);
        let (oa, ob) = (
            a["orders"].as_array().unwrap(),
            b["orders"].as_array().unwrap(),
        );
        assert!(oa.len() <= 5 && oa.len() == ob.len().min(5));
        for (i, (x, y)) in oa.iter().zip(ob.iter()).enumerate() {
            assert_eq!(x["id"], y["id"]);
            assert_eq!(x["peak_score"], y["peak_score"]);
            assert_eq!(x["explanation"], serde_json::json!([]));
            let (tx, ty) = (
                x["trades"].as_array().unwrap(),
                y["trades"].as_array().unwrap(),
            );
            if i == 0 {
                assert_eq!(tx.len(), ty.len().min(8));
                assert_eq!(
                    tx[..],
                    ty[ty.len() - tx.len()..],
                    "last trades of the newest order"
                );
            } else {
                assert!(tx.is_empty());
            }
        }
    }
    assert!(syms(&full)
        .iter()
        .flat_map(|s| s["orders"].as_array().unwrap().clone())
        .any(|o| !o["explanation"].as_array().unwrap().is_empty()));
    server.abort();
}

/// v1.1: follow-through on orders + accuracy table, timeline (`at`), sector heatmap.
#[tokio::test(flavor = "multi_thread")]
async fn view_follow_through_timeline_and_sectors() {
    let tmp = tempfile::tempdir().unwrap();
    let (base, platform, server) = serve(settings(tmp.path())).await;
    let c = reqwest::Client::new();
    let url = format!("{base}/api/tools/large_order_radar/view");
    let get = |q: &str| c.get(format!("{url}{q}")).send();

    // wait until some orders have a 5-minute outcome (synthetic market at 400x)
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut v = Value::Null;
    while Instant::now() < deadline {
        v = get("").await.unwrap().json().await.unwrap();
        let resolved = v["symbols"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|s| s["orders"].as_array().unwrap())
            .any(|o| o["outcome"]["5m"].is_number());
        if resolved {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let order = v["symbols"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["orders"].as_array().unwrap())
        .find(|o| o["outcome"]["5m"].is_number())
        .expect("an order with a 5-minute follow-through");
    assert!(order["outcome"]["ref_price"].as_f64().unwrap() > 0.0);
    assert!(order["outcome"].get("now").is_some());
    let rows = v["stats"]["rows"].as_array().expect("accuracy table");
    let all = rows.iter().find(|r| r["key"] == "all").expect("all row");
    assert!(all["n"][0].as_u64().unwrap() >= 1);
    assert_eq!(
        v["stats"]["horizons"],
        serde_json::json!(["5m", "15m", "30m"])
    );

    // timeline: a past moment only shows signals up to then, without "now" or stats
    let first = v["symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["last_signal_at"].as_str())
        .min()
        .unwrap()
        .to_string();
    // +1 s: `at` has second resolution and the signal has milliseconds
    let t = chrono::DateTime::parse_from_rfc3339(&first).unwrap() + chrono::Duration::seconds(1);
    let at = t
        .with_timezone(&chrono_tz::Asia::Ho_Chi_Minh)
        .format("%H:%M:%S")
        .to_string();
    let bound = t.with_nanosecond(0).unwrap();
    let past: Value = get(&format!("?at={at}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(past["at"].is_string());
    assert!(past.get("stats").is_none());
    let syms = past["symbols"].as_array().unwrap();
    assert!(
        !syms.is_empty(),
        "the earliest symbol is on the radar at {at}"
    );
    for s in syms {
        let last =
            chrono::DateTime::parse_from_rfc3339(s["last_signal_at"].as_str().unwrap()).unwrap();
        assert!(last <= bound, "{last} after {bound}");
        for o in s["orders"].as_array().unwrap() {
            assert!(o["outcome"].is_null() || o["outcome"]["now"].is_null());
        }
    }
    assert_eq!(get("?at=25:99").await.unwrap().status(), 422);

    // sector heatmap: every sector adds up its symbols
    let sec: Value = get("?sectors=1").await.unwrap().json().await.unwrap();
    let sectors = sec["sectors"].as_array().expect("sectors");
    assert!(!sectors.is_empty());
    for s in sectors {
        let sum: i64 = s["symbols"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x["net_large"].as_i64().unwrap())
            .sum();
        assert!(
            (sum - s["net_large"].as_i64().unwrap()).abs()
                <= s["symbols"].as_array().unwrap().len() as i64
        );
        assert!(s["n_active"].as_u64().unwrap() <= s["n_symbols"].as_u64().unwrap());
    }
    let nets: Vec<i64> = sectors
        .iter()
        .map(|s| s["net_large"].as_i64().unwrap())
        .collect();
    assert!(
        nets.windows(2).all(|w| w[0] >= w[1]),
        "sorted from net buying to net selling"
    );

    // market flow curve: buckets aligned, cumulative net adds up, totals match
    let f: Value = get("?flow=1&bucket=15")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(f["bucket_min"], 15);
    let buckets = f["buckets"].as_array().expect("buckets");
    assert!(!buckets.is_empty());
    let (mut cum, mut buy) = (0i64, 0i64);
    let mut prev = String::new();
    for b in buckets {
        let t = b["t"].as_str().unwrap().to_string();
        assert!(t > prev, "buckets ascending");
        assert_eq!(t[3..].parse::<u32>().unwrap() % 15, 0, "aligned to 15 min");
        cum += b["net"].as_i64().unwrap();
        buy += b["buy"].as_i64().unwrap();
        assert!((cum - b["cum_net"].as_i64().unwrap()).abs() <= buckets.len() as i64);
        prev = t;
    }
    assert!((buy - f["totals"]["large_buy"].as_i64().unwrap()).abs() <= buckets.len() as i64);
    // unsupported bucket falls back to 5 min; `at` cuts the curve
    let f5: Value = get("?flow=1&bucket=7").await.unwrap().json().await.unwrap();
    assert_eq!(f5["bucket_min"], 5);
    let early: Value = get("?flow=1&at=09:00").await.unwrap().json().await.unwrap();
    assert!(early["buckets"].as_array().unwrap().len() <= f5["buckets"].as_array().unwrap().len());
    platform.stop().await;
    server.abort();
}

/// Pages are revalidated on every load (a deploy shows up at once, also in iframes on
/// other sites); hashed assets are cached for a year.
#[tokio::test(flavor = "multi_thread")]
async fn dashboard_cache_headers() {
    let tmp = tempfile::tempdir().unwrap();
    let dist = tmp.path().join("dist");
    std::fs::create_dir_all(dist.join("assets")).unwrap();
    std::fs::write(dist.join("index.html"), "<!doctype html><title>t</title>").unwrap();
    std::fs::write(dist.join("assets/index-abc123.js"), "console.log(1)").unwrap();
    let mut s = settings(tmp.path());
    s.api.frontend_dist = dist.to_string_lossy().into_owned();
    let (base, platform, server) = serve(s).await;
    let c = reqwest::Client::new();
    for page in ["/", "/embed", "/embed/sectors"] {
        let r = c.get(format!("{base}{page}")).send().await.unwrap();
        assert_eq!(r.status(), 200, "{page}");
        assert_eq!(r.headers()["cache-control"], "no-cache", "{page}");
    }
    let r = c
        .get(format!("{base}/assets/index-abc123.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.headers()["cache-control"],
        "public, max-age=31536000, immutable"
    );
    // API responses keep their own policy
    let r = c
        .get(format!("{base}/api/tools/large_order_radar/view"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["cache-control"], "no-cache");
    assert!(c
        .get(format!("{base}/health"))
        .send()
        .await
        .unwrap()
        .headers()
        .get("cache-control")
        .is_none());
    platform.stop().await;
    server.abort();
}

/// End-of-day archive + viewing a past session (`?date=`), across a restart.
#[tokio::test(flavor = "multi_thread")]
async fn past_sessions_are_viewable_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let (base, platform, server) = serve(settings(tmp.path())).await;
    let c = reqwest::Client::new();
    // run until some orders have a 5-minute outcome
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let v: Value = c
            .get(format!("{base}/api/tools/large_order_radar/view"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if v["symbols"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|s| s["orders"].as_array().unwrap())
            .any(|o| o["outcome"]["5m"].is_number())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let h: Value = c
        .get(format!("{base}/api/history"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let live: chrono::NaiveDate = h["live_date"].as_str().unwrap().parse().unwrap();
    let runner = platform.runners["large_order_radar"].clone();
    platform.stop().await; // final snapshot + archive
    server.abort();
    let archive = runner.archive_path(live).unwrap();
    assert!(archive.exists(), "archive written at stop: {archive:?}");

    // Pretend that session was yesterday: rename its files and the date inside the archive.
    let past = live - chrono::Duration::days(1);
    let data = tmp.path();
    std::fs::rename(
        data.join(format!("signals/{live}.jsonl")),
        data.join(format!("signals/{past}.jsonl")),
    )
    .unwrap();
    let raw = astrq::core::remote_store::gunzip(&std::fs::read(&archive).unwrap()).unwrap();
    let raw = String::from_utf8(raw).unwrap().replacen(
        &format!("\"trading_date\":\"{live}\""),
        &format!("\"trading_date\":\"{past}\""),
        1,
    );
    std::fs::write(
        runner.archive_path(past).unwrap(),
        astrq::core::remote_store::gzip(raw.as_bytes()),
    )
    .unwrap();
    std::fs::remove_file(&archive).unwrap();
    let _ = std::fs::remove_dir_all(data.join("snapshots/large_order_radar.snap"));

    // restart: the live radar starts empty, the past session is listed and viewable
    let (base, platform, server) = serve(settings(tmp.path())).await;
    let get = |q: &str| c.get(format!("{base}{q}")).send();
    let h: Value = get("/api/history").await.unwrap().json().await.unwrap();
    assert!(
        h["dates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d == &past.to_string()),
        "{h}"
    );
    let v: Value = get(&format!("/api/tools/large_order_radar/view?date={past}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["archived"], true);
    assert_eq!(v["has_outcomes"], true);
    assert_eq!(v["trading_date"], past.to_string());
    let syms = v["symbols"].as_array().unwrap();
    assert!(!syms.is_empty());
    assert!(
        syms.iter()
            .flat_map(|s| s["orders"].as_array().unwrap())
            .any(|o| o["outcome"]["5m"].is_number()),
        "follow-through restored from the archive"
    );
    assert!(v["stats"]["rows"].as_array().is_some_and(|r| !r.is_empty()));
    // every mode works on the past session
    let sym = syms[0]["symbol"].as_str().unwrap();
    for q in [
        format!("?date={past}&symbol={sym}"),
        format!("?date={past}&sectors=1"),
        format!("?date={past}&flow=1&bucket=15"),
        format!("?date={past}&at=09:30"),
    ] {
        let r = get(&format!("/api/tools/large_order_radar/view{q}"))
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{q}");
        let v: Value = r.json().await.unwrap();
        assert_eq!(v["trading_date"], past.to_string(), "{q}");
    }
    let sigs: Value = get(&format!(
        "/api/signals?tool_id=large_order_radar&limit=50&date={past}"
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    let sigs = sigs.as_array().unwrap();
    assert!(!sigs.is_empty() && sigs.len() <= 50);
    // bad and unknown dates
    assert_eq!(
        get("/api/tools/large_order_radar/view?date=13-11-2026")
            .await
            .unwrap()
            .status(),
        422
    );
    assert_eq!(
        get("/api/tools/large_order_radar/view?date=2001-01-01")
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        get("/api/tools/large_order_radar/view?date=../../etc")
            .await
            .unwrap()
            .status(),
        422
    );
    platform.stop().await;
    server.abort();
}

/// Abuse limits: per-IP rate limit, normalized cache keys, /metrics, WebSocket quota,
/// slow-loris header timeout.
#[tokio::test(flavor = "multi_thread")]
async fn abuse_limits() {
    let tmp = tempfile::tempdir().unwrap();
    let mut s = settings(tmp.path());
    s.api.limits.per_ip_per_sec = 2.0;
    s.api.limits.per_ip_burst = 6.0;
    s.api.limits.max_ws_per_ip = 1;
    s.api.limits.header_timeout_sec = 1.0;
    let (base, platform, server) = serve(s).await;
    let c = reqwest::Client::new();

    // rate limit: the burst passes, then 429 with Retry-After; /health is never limited
    let mut codes = vec![];
    for _ in 0..10 {
        let r = c.get(format!("{base}/api/status")).send().await.unwrap();
        if r.status() == 429 {
            assert!(r.headers().contains_key("retry-after"));
        }
        codes.push(r.status().as_u16());
    }
    assert!(codes[..6].iter().all(|c| *c == 200), "{codes:?}");
    assert!(codes.contains(&429), "{codes:?}");
    for _ in 0..20 {
        assert_eq!(
            c.get(format!("{base}/health"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    // another client (behind our local proxy) has its own budget
    let other = c
        .get(format!("{base}/api/status"))
        .header("cf-connecting-ip", "198.51.100.77")
        .send()
        .await
        .unwrap();
    assert_eq!(other.status(), 200);

    // /metrics: direct local request yes, through the proxy no
    let as_client = |ip: &'static str| {
        let c = c.clone();
        let base = base.clone();
        async move {
            c.get(format!("{base}/metrics"))
                .header("cf-connecting-ip", ip)
                .send()
                .await
                .unwrap()
                .status()
        }
    };
    assert_eq!(as_client("198.51.100.78").await, 404);

    // unknown symbols are rejected instead of creating cache entries
    let r = c
        .get(format!(
            "{base}/api/tools/large_order_radar/view?symbol=NOPE123"
        ))
        .header("cf-connecting-ip", "198.51.100.79")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    // WebSocket: one live connection per client here
    let ws_url = base.replace("http://", "ws://") + "/ws";
    tokio::time::sleep(Duration::from_secs(4)).await; // refill the local bucket
    let (_ws1, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();
    let second = tokio_tungstenite::connect_async(&ws_url).await;
    assert!(
        second.is_err(),
        "second WebSocket from the same client refused"
    );

    // slow-loris: headers never finished → the server hangs up
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = base.trim_start_matches("http://").to_string();
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sock.write_all(b"GET /api/status HTTP/1.1\r\nHost: x\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 256];
    let closed = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut buf)).await;
    assert!(
        matches!(closed, Ok(Ok(_)) | Ok(Err(_))),
        "connection closed after the header timeout"
    );
    platform.stop().await;
    server.abort();
}

/// Indicator screener: catalog, presets, scans on (synthetic) daily bars, validation.
#[tokio::test(flavor = "multi_thread")]
async fn indicator_screener_on_demand() {
    let tmp = tempfile::tempdir().unwrap();
    let (base, platform, server) = serve(settings(tmp.path())).await;
    let c = reqwest::Client::new();
    let meta: Value = c
        .get(format!("{base}/api/screener/meta"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(meta["source"], "synthetic");
    assert!(meta["indicators"]
        .as_array()
        .unwrap()
        .iter()
        .any(|i| i["id"] == "rsi"));
    let presets = meta["presets"].as_array().unwrap().clone();
    assert!(presets.len() >= 5);

    let run = |body: Value| {
        let c = c.clone();
        let url = format!("{base}/api/screener/run");
        async move { c.post(url).json(&body).send().await.unwrap() }
    };
    // a wide screen matches many symbols, the opposite one the complement
    let up = run(serde_json::json!({"conditions": [{"left": {"ind": "close"}, "op": "gt", "right": {"ind": "sma", "n": 50}}]})).await;
    assert_eq!(up.status(), 200);
    let up: Value = up.json().await.unwrap();
    let down: Value = run(serde_json::json!({"conditions": [{"left": {"ind": "close"}, "op": "lte", "right": {"ind": "sma", "n": 50}}]}))
        .await
        .json()
        .await
        .unwrap();
    let scanned = up["scanned"].as_u64().unwrap();
    assert!(scanned >= 90, "{up}");
    assert_eq!(
        up["matched"].as_u64().unwrap() + down["matched"].as_u64().unwrap(),
        scanned
    );
    let row = &up["rows"][0];
    assert!(row["close"].as_f64().unwrap() > 0.0);
    assert!(row["values"]["sma(50)"].as_f64().unwrap() < row["close"].as_f64().unwrap());
    assert!(row["spark"].as_array().unwrap().len() == 40);
    assert_eq!(up["columns"][0]["label"], "MA50");

    // every preset runs
    for p in &presets {
        let r = run(serde_json::json!({"conditions": p["conditions"]})).await;
        assert_eq!(r.status(), 200, "{}", p["id"]);
    }
    // restricted to a watchlist; unknown symbols ignored
    let w: Value = run(
        serde_json::json!({"symbols": ["FPT", "VCB", "NOPE"], "logic": "any",
        "conditions": [{"left": {"ind": "rsi"}, "op": "gte", "right": {"value": 0}}]}),
    )
    .await
    .json()
    .await
    .unwrap();
    assert_eq!(w["requested"], 2);
    // validation
    for bad in [
        serde_json::json!({"conditions": []}),
        serde_json::json!({"conditions": [{"left": {"ind": "rsi", "n": 9999}, "op": "gt", "right": {"value": 1}}]}),
        serde_json::json!({"conditions": [{"left": {"ind": "evil"}, "op": "gt", "right": {"value": 1}}]}),
        serde_json::json!({"nonsense": true, "conditions": "x"}),
    ] {
        assert_eq!(run(bad.clone()).await.status(), 422, "{bad}");
    }
    let big = "x".repeat(40_000);
    let r = c
        .post(format!("{base}/api/screener/run"))
        .header("content-type", "application/json")
        .body(format!("{{\"logic\":\"{big}\"}}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 413, "body limit");
    platform.stop().await;
    server.abort();
}
