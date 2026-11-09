//! Minimal HTTP load generator for local benchmarks (not shipped in the binary).
//!
//!   cargo run --release --example loadtest -- <url> [concurrency=50] [seconds=10]
//!
//! Sends `Accept-Encoding: gzip` like a browser and reports throughput, latency
//! percentiles and bytes on the wire.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let url = args
        .get(1)
        .expect("usage: loadtest <url> [conc] [secs]")
        .clone();
    let conc: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(50);
    let secs: u64 = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(10);

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(conc)
        .build()
        .unwrap();
    let bytes = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut tasks = Vec::new();
    for _ in 0..conc {
        let (client, url, bytes, errors) =
            (client.clone(), url.clone(), bytes.clone(), errors.clone());
        tasks.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            while Instant::now() < deadline {
                let t = Instant::now();
                let r = client
                    .get(&url)
                    .header("accept-encoding", "gzip")
                    .send()
                    .await;
                match r {
                    Ok(resp) if resp.status().is_success() => match resp.bytes().await {
                        Ok(b) => {
                            bytes.fetch_add(b.len() as u64, Ordering::Relaxed);
                            lat.push(t.elapsed());
                        }
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    },
                    _ => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            lat
        }));
    }
    let mut all: Vec<Duration> = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    all.sort();
    let n = all.len().max(1);
    let pct = |p: f64| all[((n - 1) as f64 * p) as usize].as_secs_f64() * 1e3;
    println!(
        "{} req in {secs}s → {:.0} req/s · p50 {:.2} ms · p99 {:.2} ms · max {:.2} ms · {:.1} KB/resp (wire) · errors {}",
        all.len(),
        all.len() as f64 / secs as f64,
        pct(0.5),
        pct(0.99),
        pct(1.0),
        bytes.load(Ordering::Relaxed) as f64 / n as f64 / 1024.0,
        errors.load(Ordering::Relaxed)
    );
}
