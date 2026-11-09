//! Radar configuration. Every threshold is configurable; defaults are starting points.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::core::models::Severity;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct ScoreWeights {
    pub value: f64,
    pub relative: f64,
    pub velocity: f64,
    pub concentration: f64,
    pub impact: f64,
    pub side_confidence: f64,
}

impl Default for ScoreWeights {
    fn default() -> Self {
        Self {
            value: 0.28,
            relative: 0.24,
            velocity: 0.14,
            concentration: 0.12,
            impact: 0.12,
            side_confidence: 0.10,
        }
    }
}

impl ScoreWeights {
    pub fn get(&self, factor: &str) -> f64 {
        match factor {
            "value" => self.value,
            "relative" => self.relative,
            "velocity" => self.velocity,
            "concentration" => self.concentration,
            "impact" => self.impact,
            "side_confidence" => self.side_confidence,
            _ => 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct RadarConfig {
    /// Ngưỡng tuyệt đối (VND)
    pub abs_threshold_value: f64,
    /// Lớn nếu ≥ k × trung vị giá trị lệnh của mã
    pub relative_k: f64,
    /// k theo nhóm thanh khoản
    pub tier_relative_k: BTreeMap<String, f64>,
    /// Sàn giá trị cho ngưỡng tương đối
    pub relative_min_value: f64,
    /// Lệnh con tối thiểu khi chưa có baseline
    pub child_min_value: f64,
    /// Lệnh con ≥ k × trung vị (khi có baseline)
    pub child_relative_k: f64,
    /// Sàn giá trị lệnh con
    pub child_floor_value: f64,
    /// Khoảng cách tối đa giữa hai lệnh con
    pub group_gap_sec: f64,
    /// Dải giá cho phép (số bước giá)
    pub price_band_ticks: u32,
    /// Ngưỡng mở nhóm tối đa / khi thiếu baseline
    pub group_open_value: f64,
    /// Ngưỡng mở nhóm = k × trung vị (kẹp)
    pub group_relative_k: f64,
    /// Phát UPDATE khi tổng tăng thêm tỷ lệ này
    pub update_step_ratio: f64,
    /// Đóng nhóm sau khoảng nhàn rỗi
    pub idle_close_sec: f64,
    pub emit_close: bool,
    pub max_trades_in_features: usize,
    /// Ngưỡng lệnh lớn trong ATO/ATC
    pub auction_min_value: f64,
    pub auction_relative_k: f64,
    /// Ngưỡng hiển thị thỏa thuận
    pub putthrough_min_value: f64,
    /// Mức độ tối đa của tín hiệu thỏa thuận
    pub putthrough_max_severity: Severity,
    pub metrics_window_sec: f64,
    pub quote_stale_sec: f64,
    pub weights: ScoreWeights,
    pub notice_score: f64,
    pub alert_score: f64,
    /// Tín hiệu hấp thụ: lệnh lớn dồn một chiều nhưng giá không đi theo
    pub absorption: AbsorptionConfig,
    /// Tín hiệu gom/xả âm thầm: nhiều lệnh vừa, đều đặn, cùng chiều
    pub accumulation: AccumulationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct AbsorptionConfig {
    pub enabled: bool,
    /// Giá trị lệnh lớn bị hấp thụ tối thiểu = k × ngưỡng mở nhóm của mã
    pub min_value_k: f64,
    /// Tỷ trọng tối thiểu của chiều bị hấp thụ trong giá trị lệnh lớn của cửa sổ
    pub min_share: f64,
    /// Số lệnh lớn tối thiểu của chiều bị hấp thụ
    pub min_trades: u32,
    /// Áp lực phải kéo dài tối thiểu (giây) — loại một lệnh mẹ khớp nhanh
    pub min_duration_sec: f64,
    /// Giá đi theo chiều lệnh lớn không quá N bước giá (1 bước = dao động mua/bán)
    pub max_move_ticks: f64,
    /// Phát UPDATE khi giá trị bị hấp thụ tăng thêm tỷ lệ này
    pub update_step_ratio: f64,
    /// Nghỉ giữa hai đợt cùng chiều của một mã (giây)
    pub cooldown_sec: f64,
}

impl Default for AbsorptionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_value_k: 2.0,
            min_share: 0.7,
            min_trades: 5,
            min_duration_sec: 180.0,
            max_move_ticks: 1.0,
            update_step_ratio: 0.5,
            cooldown_sec: 900.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct AccumulationConfig {
    pub enabled: bool,
    /// Cửa sổ theo dõi (giây)
    pub window_sec: f64,
    /// Lệnh "vừa" ≥ k × trung vị giá trị lệnh (dưới ngưỡng lệnh lớn)
    pub medium_k: f64,
    /// Sàn giá trị lệnh vừa (VND); dùng khi chưa có baseline
    pub medium_min_value: f64,
    /// Số lệnh vừa cùng chiều tối thiểu trong cửa sổ
    pub min_trades: u32,
    /// Tổng giá trị tối thiểu = k × ngưỡng mở nhóm của mã
    pub min_value_k: f64,
    /// Tỷ trọng tối thiểu của chiều chiếm ưu thế trong giá trị lệnh vừa
    pub min_share: f64,
    /// Đều đặn: hệ số biến thiên khối lượng ≤ mức này, hoặc
    pub max_volume_cv: f64,
    /// … một mức khối lượng lặp lại chiếm ≥ tỷ lệ này số lệnh
    pub min_repeat_share: f64,
    pub update_step_ratio: f64,
    pub cooldown_sec: f64,
}

impl Default for AccumulationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            window_sec: 1800.0,
            medium_k: 2.0,
            medium_min_value: 50e6,
            min_trades: 10,
            min_value_k: 1.0,
            min_share: 0.75,
            max_volume_cv: 0.35,
            min_repeat_share: 0.4,
            update_step_ratio: 0.5,
            cooldown_sec: 1800.0,
        }
    }
}

impl Default for RadarConfig {
    fn default() -> Self {
        Self {
            abs_threshold_value: 500e6,
            relative_k: 10.0,
            tier_relative_k: [("high", 12.0), ("mid", 10.0), ("low", 8.0)]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            relative_min_value: 150e6,
            child_min_value: 100e6,
            child_relative_k: 4.0,
            child_floor_value: 50e6,
            group_gap_sec: 30.0,
            price_band_ticks: 3,
            group_open_value: 1.5e9,
            group_relative_k: 25.0,
            update_step_ratio: 0.5,
            idle_close_sec: 90.0,
            emit_close: true,
            max_trades_in_features: 40,
            auction_min_value: 2e9,
            auction_relative_k: 60.0,
            putthrough_min_value: 1e9,
            putthrough_max_severity: Severity::Notice,
            metrics_window_sec: 900.0,
            quote_stale_sec: 15.0,
            weights: ScoreWeights::default(),
            notice_score: 40.0,
            alert_score: 65.0,
            absorption: AbsorptionConfig::default(),
            accumulation: AccumulationConfig::default(),
        }
    }
}
