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
    let h = tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
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
        csp.starts_with("frame-ancestors 'self'") && csp.contains("https://*.astrq.io.vn"),
        "{csp}"
    );
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
