//! Deterministic replay scenarios checked against the golden files produced by the
//! original Python implementation (behavioural parity of the rewrite).
//!
//! Regenerate after an intentional behaviour change:
//!     UPDATE_GOLDEN=1 cargo test --test scenarios
//! and review the diff of tests/golden/*.json before committing.

mod common;

use common::*;
use serde_json::Value;
use std::path::PathBuf;

fn golden_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden").join(format!("{name}.json"))
}

#[tokio::test]
async fn scenarios_match_python_golden_files() {
    let mut failures = Vec::new();
    for (name, build) in scenarios() {
        let got = Value::Array(project(&run(build()).await));
        let path = golden_path(name);
        if std::env::var("UPDATE_GOLDEN").is_ok() {
            std::fs::write(&path, serde_json::to_string_pretty(&got).unwrap() + "\n").unwrap();
            continue;
        }
        let want: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        if got != want {
            failures.push(format!(
                "== {name}\n-- want\n{}\n-- got\n{}",
                serde_json::to_string_pretty(&want).unwrap(),
                serde_json::to_string_pretty(&got).unwrap()
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn scenarios_are_deterministic() {
    for (name, build) in scenarios() {
        let a: Vec<String> = run(build()).await.signals.iter().map(|s| serde_json::to_string(s).unwrap()).collect();
        let b: Vec<String> = run(build()).await.signals.iter().map(|s| serde_json::to_string(s).unwrap()).collect();
        assert_eq!(a, b, "{name}");
    }
}

#[tokio::test]
async fn single_block_opens_and_closes() {
    let sig = run(single_block()).await.signals;
    assert_eq!(sig.iter().map(|s| s.kind.as_str()).collect::<Vec<_>>(), ["block_order_open", "block_order_close"]);
    let trig = sig[0].features["group"]["triggers"].as_array().unwrap();
    assert_eq!(trig.len(), 2);
}

#[tokio::test]
async fn split_parent_is_one_group() {
    let sig = run(split_parent()).await.signals;
    assert_eq!(
        sig.iter().map(|s| s.kind.as_str()).collect::<Vec<_>>(),
        ["block_order_open", "block_order_update", "block_order_update", "block_order_close"]
    );
    let close = &sig.last().unwrap().features["group"];
    assert_eq!(close["n_trades"], 12);
    assert!(close["price_change_pct"].as_f64().unwrap() > 0.0);
    assert!(sig.iter().all(|s| s.correlation_id == sig[0].correlation_id));
}

#[tokio::test]
async fn reversal_lunch_auction_putthrough() {
    let rev = run(reversal()).await.signals;
    let opens: Vec<&str> = rev.iter().filter(|s| s.kind == "block_order_open").map(|s| s.side.as_str()).collect();
    assert_eq!(opens, ["buy", "sell"]);

    let lunch = run(lunch_break()).await.signals;
    let sessions: Vec<&str> =
        lunch.iter().filter(|s| s.kind == "block_order_open").map(|s| s.features["group"]["session"].as_str().unwrap()).collect();
    assert_eq!(sessions, ["continuous_am", "continuous_pm"]);

    let auc = run(auctions()).await.signals;
    assert_eq!(auc.iter().map(|s| (s.symbol.as_str(), s.kind.as_str())).collect::<Vec<_>>(), [("AAA", "block_auction"), ("AAA", "block_auction")]);

    let pt = run(putthrough_interleaved()).await.signals;
    let p: Vec<_> = pt.iter().filter(|s| s.kind == "block_putthrough").collect();
    assert_eq!(p.len(), 1);
    assert!(matches!(p[0].severity.as_str(), "info" | "notice"));
    let close = pt.iter().find(|s| s.kind == "block_order_close").unwrap();
    assert_eq!(close.features["group"]["n_trades"], 8);
    let m = &close.features["symbol_metrics"];
    assert_eq!(m["day_putthrough_value"], 19_500_000_000i64);
    assert_eq!(m["large_buy"], close.features["group"]["total_value"]); // put-through excluded
}

#[tokio::test]
async fn duplicates_dropped_late_flagged_and_inferred_side() {
    let res = run(duplicates_and_late()).await;
    assert_eq!(res.ingest_stats.duplicates, 1);
    assert_eq!(res.ingest_stats.late, 1);
    let close = res.signals.iter().find(|s| s.kind == "block_order_close").unwrap();
    assert_eq!(close.features["group"]["n_trades"], 9);
    assert!(close.quality_flags.contains(&"late_ticks".to_string()));

    let inf = run(inferred_side()).await.signals;
    assert!(!inf.is_empty() && inf.iter().all(|s| s.side.as_str() == "buy"));
    assert!(inf.iter().all(|s| s.quality_flags.contains(&"side_inferred".to_string())));
}
