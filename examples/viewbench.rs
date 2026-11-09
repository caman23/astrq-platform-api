//! Time each stage of the radar view endpoint on a full synthetic day.
//!
//!   astrq simulate --out data/ticks/replay.jsonl
//!   cargo run --release --example viewbench -- data/ticks/replay.jsonl

use astrq::core::config::load_settings;
use astrq::core::context::ContextService;
use astrq::core::market_rules::MarketRules;
use astrq::core::offline::run_offline;
use astrq::core::universe::Universe;
use astrq::providers::replay::read_events;
use astrq::providers::synthetic::SyntheticContextProvider;
use astrq::tools::build_tool;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let input = std::env::args()
        .nth(1)
        .expect("usage: viewbench <events.jsonl>");
    let s = load_settings(None, false)?;
    let universe = Universe::load(&s.path(&s.universe.file))?;
    let provider = Arc::new(SyntheticContextProvider {
        seed: s.provider.synthetic.seed,
        rules: MarketRules::new(&s.market_rules),
    });
    let tools = s
        .tools
        .iter()
        .filter(|(_, e)| e.enabled)
        .map(|(id, e)| build_tool(id, e).map(|t| (t, e.clone())))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let res = run_offline(
        read_events(input.as_ref())?,
        &s,
        universe,
        Arc::new(ContextService::new(provider)),
        tools,
        true,
        true,
    )
    .await?;
    let r = &res.runners["large_order_radar"];
    let signals: Vec<_> = res
        .signals
        .iter()
        .filter(|x| x.tool_id == "large_order_radar")
        .cloned()
        .map(Arc::new)
        .collect();
    println!("signals: {}", signals.len());

    // optional: argv[2] = query string (e.g. "full=1"), argv[3] = dump the JSON here
    let params: HashMap<String, String> = std::env::args()
        .nth(2)
        .unwrap_or_default()
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    if let Some(out) = std::env::args().nth(3) {
        let v = r.tool.lock().view(&signals, &r.ctx, &params).unwrap();
        std::fs::write(out, serde_json::to_vec(&v)?)?;
    }
    let n = 20;
    let (mut t_clone, mut t_view, mut t_ser, mut t_gz) = (0.0, 0.0, 0.0, 0.0);
    let (mut raw, mut gz) = (0, 0);
    for _ in 0..n {
        let t = Instant::now();
        let sigs = signals.clone();
        t_clone += t.elapsed().as_secs_f64();
        let t = Instant::now();
        let v = r.tool.lock().view(&sigs, &r.ctx, &params).unwrap();
        t_view += t.elapsed().as_secs_f64();
        let t = Instant::now();
        let body = serde_json::to_vec(&v)?;
        t_ser += t.elapsed().as_secs_f64();
        let t = Instant::now();
        let mut e = GzEncoder::new(Vec::new(), Compression::default());
        e.write_all(&body)?;
        let z = e.finish()?;
        t_gz += t.elapsed().as_secs_f64();
        raw = body.len();
        gz = z.len();
    }
    let ms = |x: f64| x / n as f64 * 1e3;
    println!(
        "per request: clone {:.2} ms · view {:.2} ms · serialize {:.2} ms · gzip {:.2} ms · total {:.2} ms",
        ms(t_clone),
        ms(t_view),
        ms(t_ser),
        ms(t_gz),
        ms(t_clone + t_view + t_ser + t_gz)
    );
    println!(
        "size: {:.0} KB raw · {:.0} KB gzip",
        raw as f64 / 1024.0,
        gz as f64 / 1024.0
    );
    Ok(())
}
