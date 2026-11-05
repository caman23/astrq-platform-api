//! Radar score: weighted, normalized factors with a full explanation.

use crate::core::context::Baseline;
use crate::core::fmt::py_round;
use crate::core::models::{Contribution, Severity, Side};
use crate::tools::large_order_radar::config::RadarConfig;

pub const FACTORS: [&str; 6] = ["value", "relative", "velocity", "concentration", "impact", "side_confidence"];

fn label(f: &str) -> &'static str {
    match f {
        "value" => "Giá trị",
        "relative" => "Độ lớn so với bình thường",
        "velocity" => "Tốc độ (giá trị/phút)",
        "concentration" => "Độ tập trung lệnh lớn",
        "impact" => "Tác động giá",
        _ => "Độ tin cậy chiều",
    }
}

fn clamp01(x: f64) -> f64 {
    x.clamp(0.0, 1.0)
}

/// 0 at x == base, 1 at x == base * span (log scale).
fn log_ratio(x: f64, base: f64, span: f64) -> f64 {
    if x <= 0.0 || base <= 0.0 {
        return 0.0;
    }
    clamp01((x / base).ln() / span.ln())
}

pub struct ScoreInput<'a> {
    pub total_value: f64,
    pub duration_sec: f64,
    pub side: Side,
    pub first_price: f64,
    pub last_price: f64,
    pub side_confidence: f64,
    pub concentration: f64,
    pub baseline: Option<&'a Baseline>,
}

pub struct ScoreResult {
    pub score: f64,
    pub severity: Severity,
    pub explanation: Vec<Contribution>,
}

/// `factors` restricts which factors apply (e.g. auctions only use value/relative).
pub fn score(inp: &ScoreInput, cfg: &RadarConfig, factors: Option<&[&str]>) -> ScoreResult {
    let mut raw: Vec<(&str, f64, f64, String)> = Vec::with_capacity(6);
    raw.push((
        "value",
        inp.total_value,
        log_ratio(inp.total_value, cfg.abs_threshold_value, 20.0),
        "0 tại ngưỡng tuyệt đối, 1 tại 20× ngưỡng".into(),
    ));
    match inp.baseline.filter(|b| b.median_trade_value > 0.0) {
        Some(b) => {
            let ratio = inp.total_value / b.median_trade_value;
            raw.push(("relative", ratio, log_ratio(ratio, 1.0, 500.0), format!("{ratio:.0}× trung vị lệnh")));
        }
        None => raw.push(("relative", 0.0, 0.0, "thiếu baseline".into())),
    }
    let minutes = (inp.duration_sec / 60.0).max(1.0);
    let vpm = inp.total_value / minutes;
    raw.push(("velocity", vpm, log_ratio(vpm, cfg.abs_threshold_value, 20.0), "giá trị mỗi phút".into()));
    raw.push((
        "concentration",
        inp.concentration,
        clamp01(inp.concentration),
        "tỷ trọng lệnh lớn trong giá trị chủ động cùng chiều".into(),
    ));
    let mv = if inp.first_price != 0.0 { (inp.last_price - inp.first_price) / inp.first_price * 100.0 } else { 0.0 };
    let direction = match inp.side {
        Side::Buy => 1.0,
        Side::Sell => -1.0,
        Side::Unknown => 0.0,
    };
    raw.push(("impact", mv, clamp01(direction * mv / 1.0), "1% theo chiều lệnh = tối đa".into()));
    raw.push(("side_confidence", inp.side_confidence, clamp01(inp.side_confidence), String::new()));

    let active: Vec<&str> = factors.map(|f| f.to_vec()).unwrap_or_else(|| FACTORS.to_vec());
    let w = &cfg.weights;
    let total_w = {
        let t: f64 = FACTORS.iter().filter(|f| active.contains(f)).map(|f| w.get(f)).sum();
        if t == 0.0 { 1.0 } else { t }
    };
    let mut acc = 0.0;
    let mut explanation = Vec::new();
    for (f, r, n, note) in raw {
        if !active.contains(&f) {
            continue;
        }
        let c = 100.0 * w.get(f) * n / total_w;
        acc += c;
        explanation.push(Contribution {
            factor: f.into(),
            label: label(f).into(),
            raw: py_round(r, 4),
            normalized: py_round(n, 4),
            weight: w.get(f),
            contribution: py_round(c, 2),
            note,
        });
    }
    let s = py_round(acc, 1);
    let severity = if s >= cfg.alert_score {
        Severity::Alert
    } else if s >= cfg.notice_score {
        Severity::Notice
    } else {
        Severity::Info
    };
    ScoreResult { score: s, severity, explanation }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn bl() -> Baseline {
        Baseline {
            symbol: "AAA".into(),
            trading_date: NaiveDate::from_ymd_opt(2026, 10, 21).unwrap(),
            median_trade_value: 20e6,
            p95_trade_value: 140e6,
            avg_daily_value: 1e10,
            liquidity_tier: "mid".into(),
            source: "t".into(),
        }
    }

    fn inp(value: f64, side: Side, last: f64, b: Option<&Baseline>) -> ScoreInput<'_> {
        ScoreInput { total_value: value, duration_sec: 120.0, side, first_price: 20_000.0, last_price: last, side_confidence: 1.0, concentration: 0.5, baseline: b }
    }

    #[test]
    fn explanation_sums_and_monotonic() {
        let cfg = RadarConfig::default();
        let b = bl();
        let r = score(&inp(3e9, Side::Buy, 20_100.0, Some(&b)), &cfg, None);
        let sum: f64 = r.explanation.iter().map(|c| c.contribution).sum();
        assert!((sum - r.score).abs() < 0.2 && (0.0..=100.0).contains(&r.score));
        let s: Vec<f64> = [0.6e9, 1e9, 3e9, 9e9].iter().map(|v| score(&inp(*v, Side::Buy, 20_100.0, Some(&b)), &cfg, None).score).collect();
        assert!(s.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn sell_impact_and_cold_baseline() {
        let cfg = RadarConfig::default();
        let b = bl();
        let up = score(&inp(2e9, Side::Sell, 20_200.0, Some(&b)), &cfg, None).score;
        let down = score(&inp(2e9, Side::Sell, 19_800.0, Some(&b)), &cfg, None).score;
        assert!(down > up);
        let cold = score(&inp(2e9, Side::Buy, 20_100.0, None), &cfg, None);
        assert_eq!(cold.explanation.iter().find(|c| c.factor == "relative").unwrap().contribution, 0.0);
    }
}
