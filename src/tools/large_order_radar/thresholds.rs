//! Absolute and per-symbol relative thresholds.

use crate::core::context::Baseline;
use crate::tools::large_order_radar::config::RadarConfig;

pub struct Thresholds<'a> {
    pub cfg: &'a RadarConfig,
}

impl Thresholds<'_> {
    pub fn relative_k(&self, b: &Baseline) -> f64 {
        self.cfg.tier_relative_k.get(&b.liquidity_tier).copied().unwrap_or(self.cfg.relative_k)
    }

    /// Which "large trade" thresholds this single trade satisfies.
    pub fn large_triggers(&self, value: f64, baseline: Option<&Baseline>) -> Vec<String> {
        let mut out = Vec::new();
        if value >= self.cfg.abs_threshold_value {
            out.push("absolute".to_string());
        }
        if let Some(b) = baseline.filter(|b| b.median_trade_value > 0.0) {
            let bar = self.cfg.relative_min_value.max(self.relative_k(b) * b.median_trade_value);
            if value >= bar {
                out.push("relative".to_string());
            }
        }
        out
    }

    /// May this trade be part of a split parent order?
    pub fn is_child_candidate(&self, value: f64, baseline: Option<&Baseline>) -> bool {
        match baseline.filter(|b| b.median_trade_value > 0.0) {
            None => value >= self.cfg.child_min_value,
            Some(b) => value >= self.cfg.child_floor_value.max(self.cfg.child_relative_k * b.median_trade_value),
        }
    }

    pub fn group_open_value(&self, baseline: Option<&Baseline>) -> f64 {
        match baseline.filter(|b| b.median_trade_value > 0.0) {
            None => self.cfg.group_open_value,
            Some(b) => {
                let rel = self.cfg.group_relative_k * b.median_trade_value;
                self.cfg.abs_threshold_value.max(self.cfg.group_open_value.min(rel))
            }
        }
    }

    pub fn auction_value(&self, baseline: Option<&Baseline>) -> f64 {
        match baseline {
            None => self.cfg.auction_min_value,
            Some(b) => self.cfg.auction_min_value.max(self.cfg.auction_relative_k * b.median_trade_value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn bl(median: f64) -> Baseline {
        Baseline {
            symbol: "AAA".into(),
            trading_date: NaiveDate::from_ymd_opt(2026, 10, 21).unwrap(),
            median_trade_value: median,
            p95_trade_value: median * 7.0,
            avg_daily_value: 1e10,
            liquidity_tier: "mid".into(),
            source: "t".into(),
        }
    }

    #[test]
    fn absolute_relative_and_clamps() {
        let cfg = RadarConfig::default();
        let th = Thresholds { cfg: &cfg };
        assert_eq!(th.large_triggers(600e6, None), vec!["absolute"]);
        assert!(th.large_triggers(300e6, None).is_empty());
        assert_eq!(th.large_triggers(250e6, Some(&bl(20e6))), vec!["relative"]);
        assert_eq!(th.large_triggers(600e6, Some(&bl(20e6))), vec!["absolute", "relative"]);
        assert_eq!(th.group_open_value(None), 1.5e9);
        assert_eq!(th.group_open_value(Some(&bl(5e6))), 500e6);
        assert_eq!(th.group_open_value(Some(&bl(40e6))), 1e9);
        assert_eq!(th.group_open_value(Some(&bl(200e6))), 1.5e9);
    }
}
