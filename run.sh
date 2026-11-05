#!/usr/bin/env bash
# Chạy Radar Lệnh Lớn trên máy local.
#
#   ./run.sh              chạy với nguồn dữ liệu trong .env (DNSE)
#   ./run.sh --demo       chạy với dữ liệu mô phỏng (không cần key, xem được ngoài giờ)
#   ./run.sh --check      kiểm tra key DNSE + nghe dữ liệu 30 giây rồi thoát
#   ./run.sh --port 8080  đổi cổng (mặc định 8000)
#   ./run.sh --no-browser không tự mở trình duyệt
#   ./run.sh --public     chạy kèm Cloudflare Tunnel -> https://radar.astrq.io.vn
#                         (đổi tên: ASTRQ_PUBLIC_HOST=ten.astrq.io.vn ./run.sh --public)
#
# Lần đầu chạy, script tự build backend Rust (bản release) và giao diện.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

MODE="live"
PORT="8000"
OPEN_BROWSER=1
PUBLIC=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --demo) MODE="demo" ;;
    --check) MODE="check" ;;
    --port) PORT="${2:?thiếu số cổng sau --port}"; shift ;;
    --no-browser) OPEN_BROWSER=0 ;;
    --public) PUBLIC=1 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "Tùy chọn không hợp lệ: $1 (xem ./run.sh --help)"; exit 1 ;;
  esac
  shift
done

say()  { printf '\033[1;36m▸ %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m✔ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m⚠ %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m✖ %s\033[0m\n' "$*" >&2; exit 1; }

# ------------------------------------------------------------------ prerequisites
export PATH="$HOME/.cargo/bin:$PATH"
command -v cargo >/dev/null || die "Chưa có Rust. Cài: curl --proto '=https' -sSf https://sh.rustup.rs | sh"
command -v node >/dev/null || die "Chưa có Node.js (cần ≥ 20) để build giao diện"

# ------------------------------------------------------------------ backend (Rust)
BIN=backend-rs/target/release/astrq
if [[ ! -x $BIN ]] || \
   [[ -n "$(find backend-rs/src backend-rs/Cargo.toml backend-rs/Cargo.lock -newer "$BIN" -print -quit)" ]]; then
  say "Build backend Rust (lần đầu mất vài phút)…"
  (cd backend-rs && cargo build --release --quiet)
  ok "Đã build backend"
fi

# ------------------------------------------------------------------ frontend build
if [[ ! -d frontend/node_modules || frontend/package.json -nt frontend/node_modules ]]; then
  say "Cài thư viện giao diện (npm install)…"
  (cd frontend && npm install --no-audit --no-fund --silent)
fi
if [[ ! -f frontend/dist/index.html ]] || \
   [[ -n "$(find frontend/src frontend/index.html -newer frontend/dist/index.html -print -quit)" ]]; then
  say "Build giao diện…"
  (cd frontend && npm run build --silent >/dev/null)
  ok "Đã build giao diện"
fi

# ------------------------------------------------------------------ configuration
env_val() { grep -E "^$1=" .env 2>/dev/null | tail -1 | cut -d= -f2- || true; }

if [[ $MODE == "demo" ]]; then
  # Shell env overrides .env, so the demo never touches the real feed.
  export ASTRQ_PROVIDER__KIND=synthetic ASTRQ_CONTEXT__KIND=synthetic \
         ASTRQ_SESSIONS__ENFORCE_TRADING_DAYS=false ASTRQ_STORAGE__DATA_DIR=data/demo
  # each demo restarts the simulated day, so yesterday's demo state must not be reloaded
  rm -rf data/demo/signals data/demo/snapshots
  ok "Chế độ mô phỏng (dữ liệu giả, lưu ở data/demo)"
else
  [[ -f .env ]] || die "Chưa có file .env — chạy: cp .env.example .env rồi điền key DNSE"
  KIND="$(env_val ASTRQ_PROVIDER__KIND)"
  if [[ $KIND == "dnse" || $MODE == "check" ]]; then
    [[ -n "$(env_val ASTRQ_PROVIDER__DNSE__API_KEY)" ]] || die "Thiếu ASTRQ_PROVIDER__DNSE__API_KEY trong .env"
    [[ -n "$(env_val ASTRQ_PROVIDER__DNSE__API_SECRET)" ]] || die "Thiếu ASTRQ_PROVIDER__DNSE__API_SECRET trong .env"
    ok "Key DNSE đã có trong .env"
  fi
fi

if [[ $MODE == "check" ]]; then
  say "Kiểm tra kết nối DNSE (30 giây)…"
  exec "$BIN" dnse-check --listen 30
fi

# ------------------------------------------------------------------ market hours hint
VN_TIME="$(TZ=Asia/Ho_Chi_Minh date '+%H%M')"
VN_DOW="$(TZ=Asia/Ho_Chi_Minh date '+%u')"
if [[ $MODE == "live" ]]; then
  if (( VN_DOW >= 6 )); then
    warn "Hôm nay là cuối tuần — thị trường đóng, radar sẽ trống. Xem thử: ./run.sh --demo"
  elif (( 10#$VN_TIME < 900 )); then
    ok "Trước giờ mở cửa — để chạy, radar sẽ bắt đầu từ phiên ATO 9:00"
  elif (( 10#$VN_TIME >= 1500 )); then
    warn "Đã hết phiên — radar trống cho tới 9:00 phiên sau. Xem thử: ./run.sh --demo"
  fi
fi

# ------------------------------------------------------------------ run
if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1 || ss -ltn 2>/dev/null | grep -q ":$PORT "; then
  die "Cổng $PORT đang bị dùng (có thể radar đã chạy ở cửa sổ khác). Dùng: ./run.sh --port 8080"
fi

mkdir -p data/logs
LOG="data/logs/$(TZ=Asia/Ho_Chi_Minh date +%F).log"
URL="http://127.0.0.1:$PORT"

if (( OPEN_BROWSER )); then
  (
    for _ in $(seq 1 60); do
      if curl -fsS "$URL/health" >/dev/null 2>&1; then
        { xdg-open "$URL" || open "$URL"; } >/dev/null 2>&1 || true
        exit 0
      fi
      sleep 0.5
    done
  ) &
fi

if (( PUBLIC )); then
  PUBLIC_HOST="${ASTRQ_PUBLIC_HOST:-radar.astrq.io.vn}"
  if [[ ! -f .tunnel/access-confirmed ]]; then
    warn "Dashboard chưa có đăng nhập. Đưa lên https://$PUBLIC_HOST mà không bật Cloudflare Access"
    warn "thì ai có link cũng xem được dữ liệu DNSE của bạn (xem scripts/tunnel-setup.sh)."
    read -r -p "Đã bật Cloudflare Access cho $PUBLIC_HOST chưa? [y/N] " ans
    [[ $ans =~ ^[yY]$ ]] || die "Dừng. Bật Access xong chạy lại ./run.sh --public"
    mkdir -p .tunnel && touch .tunnel/access-confirmed
  fi
  scripts/tunnel-setup.sh --quiet --host "$PUBLIC_HOST" --port "$PORT"
  TUNNEL_LOG="data/logs/tunnel-$(TZ=Asia/Ho_Chi_Minh date +%F).log"
  cloudflared tunnel --config .tunnel/config.yml --loglevel info --logfile "$TUNNEL_LOG" run \
    >/dev/null 2>&1 &
  TUNNEL_PID=$!
  trap 'kill "$TUNNEL_PID" 2>/dev/null || true' EXIT INT TERM
  ok "Tunnel đang chạy → https://$PUBLIC_HOST (log: $TUNNEL_LOG)"
fi

say "Radar chạy tại $URL — Ctrl+C để dừng · log: $LOG"
"$BIN" serve --port "$PORT" --plain-logs 2>&1 | tee -a "$LOG"
