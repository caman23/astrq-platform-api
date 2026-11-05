//! Exchange rules (tick size ladder) loaded from config.

use std::collections::HashMap;

use crate::core::config::MarketRulesConfig;

#[derive(Debug, Clone)]
pub struct MarketRules {
    by_exchange: HashMap<String, Vec<(f64, f64)>>,
}

impl MarketRules {
    pub fn new(cfg: &MarketRulesConfig) -> Self {
        let mut by_exchange: HashMap<String, Vec<(f64, f64)>> = HashMap::new();
        for r in &cfg.tick_sizes {
            by_exchange
                .entry(r.exchange.to_ascii_uppercase())
                .or_default()
                .push((r.below.unwrap_or(f64::INFINITY), r.size));
        }
        for rules in by_exchange.values_mut() {
            rules.sort_by(|a, b| a.0.total_cmp(&b.0));
        }
        Self { by_exchange }
    }

    pub fn tick_size(&self, price: f64, exchange: &str) -> f64 {
        let Some(rules) = self.by_exchange.get(&exchange.to_ascii_uppercase()) else { return 10.0 };
        rules.iter().find(|(bound, _)| price < *bound).map(|r| r.1).unwrap_or_else(|| rules.last().map_or(10.0, |r| r.1))
    }

    pub fn round_to_tick(&self, price: f64, exchange: &str) -> f64 {
        let step = self.tick_size(price, exchange);
        crate::core::fmt::py_round((price / step).round_ties_even() * step, 2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hose_ladder() {
        let r = MarketRules::new(&MarketRulesConfig::default());
        assert_eq!(r.tick_size(9_990.0, "HOSE"), 10.0);
        assert_eq!(r.tick_size(10_000.0, "HOSE"), 50.0);
        assert_eq!(r.tick_size(49_950.0, "hose"), 50.0);
        assert_eq!(r.tick_size(50_000.0, "HOSE"), 100.0);
    }
}
