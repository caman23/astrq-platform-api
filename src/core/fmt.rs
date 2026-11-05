//! Vietnamese display formatting (user-facing strings) and Python-compatible rounding.

use chrono::{DateTime, Utc};
use chrono_tz::Asia::Ho_Chi_Minh;

pub const DISCLAIMER_VI: &str = "Thông tin chỉ mang tính tham khảo, không phải khuyến nghị đầu tư.";

pub fn side_vi(side: &str) -> &'static str {
    match side {
        "buy" => "MUA",
        "sell" => "BÁN",
        _ => "KHÔNG RÕ CHIỀU",
    }
}

pub fn severity_vi(sev: &str) -> &'static str {
    match sev {
        "info" => "Thông tin",
        "notice" => "Chú ý",
        "alert" => "Cảnh báo",
        _ => "",
    }
}

pub fn flag_vi(flag: &str) -> String {
    match flag {
        "baseline_cold" => "thiếu dữ liệu nền (độ tin cậy thấp hơn)",
        "side_inferred" => "chiều suy luận",
        "side_unknown" => "không xác định chiều",
        "inside_spread" => "khớp trong spread",
        "late_ticks" => "có tick đến muộn",
        "auction" => "phiên khớp lệnh định kỳ",
        "negotiated" => "giao dịch thỏa thuận",
        other => return other.to_string(),
    }
    .to_string()
}

/// Round like Python's `round(x, nd)` (correctly rounded, ties to even on exact ties).
pub fn py_round(x: f64, nd: usize) -> f64 {
    format!("{x:.nd$}").parse().unwrap_or(x)
}

/// Python's `round(x)` for floats -> integer (banker's rounding on exact ties).
pub fn py_round_int(x: f64) -> i64 {
    x.round_ties_even() as i64
}

fn vn_number(x: f64, decimals: usize) -> String {
    let s = format!("{:.*}", decimals, x.abs());
    let (int_part, frac) = match s.split_once('.') {
        Some((i, f)) => (i.to_string(), Some(f.to_string())),
        None => (s.clone(), None),
    };
    let mut grouped = String::new();
    for (i, ch) in int_part.chars().enumerate() {
        if i > 0 && (int_part.len() - i) % 3 == 0 {
            grouped.push('.');
        }
        grouped.push(ch);
    }
    let sign = if x < 0.0 && s.chars().any(|c| c.is_ascii_digit() && c != '0') { "-" } else { "" };
    match frac {
        Some(f) => format!("{sign}{grouped},{f}"),
        None => format!("{sign}{grouped}"),
    }
}

/// VND value: "1,25 tỷ" / "520 tr".
pub fn vnd(value: f64) -> String {
    let a = value.abs();
    if a >= 1e9 {
        format!("{} tỷ", vn_number(value / 1e9, 2))
    } else if a >= 1e6 {
        format!("{} tr", vn_number(value / 1e6, 0))
    } else {
        vn_number(value, 0)
    }
}

pub fn price(p: f64) -> String {
    vn_number(p, 0)
}

pub fn hms(ts: DateTime<Utc>) -> String {
    ts.with_timezone(&Ho_Chi_Minh).format("%H:%M:%S").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_like_the_dashboard() {
        assert_eq!(vnd(1_250_000_000.0), "1,25 tỷ");
        assert_eq!(vnd(520_400_000.0), "520 tr");
        assert_eq!(vnd(-73_000_000.0), "-73 tr");
        assert_eq!(price(124_000.0), "124.000");
        assert_eq!(vnd(19_500_000_000.0), "19,50 tỷ");
    }

    #[test]
    fn python_rounding() {
        assert_eq!(py_round(40.65, 1), 40.6); // 40.65 is 40.649999.. in binary, like Python
        assert_eq!(py_round(2.675, 2), 2.67);
        assert_eq!(py_round_int(2.5), 2);
        assert_eq!(py_round_int(3.5), 4);
    }
}
