"""Vietnamese display formatting helpers (user-facing strings)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

SIDE_VI = {"buy": "MUA", "sell": "BÁN", "unknown": "KHÔNG RÕ CHIỀU"}
SEVERITY_VI = {"info": "Thông tin", "notice": "Chú ý", "alert": "Cảnh báo"}
FLAG_VI = {
    "baseline_cold": "thiếu dữ liệu nền (độ tin cậy thấp hơn)",
    "side_inferred": "chiều suy luận",
    "side_unknown": "không xác định chiều",
    "inside_spread": "khớp trong spread",
    "late_ticks": "có tick đến muộn",
    "auction": "phiên khớp lệnh định kỳ",
    "negotiated": "giao dịch thỏa thuận",
}
DISCLAIMER_VI = "Thông tin chỉ mang tính tham khảo, không phải khuyến nghị đầu tư."


def _vn_number(x: float, decimals: int) -> str:
    s = f"{x:,.{decimals}f}"
    return s.replace(",", "_").replace(".", ",").replace("_", ".")


def vnd(value: float) -> str:
    a = abs(value)
    if a >= 1e9:
        return f"{_vn_number(value / 1e9, 2)} tỷ"
    if a >= 1e6:
        return f"{_vn_number(value / 1e6, 0)} tr"
    return _vn_number(value, 0)


def price(p: float) -> str:
    return _vn_number(p, 0)


def hms(ts: datetime) -> str:
    return ts.astimezone(VN_TZ).strftime("%H:%M:%S")
