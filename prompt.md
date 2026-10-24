# PROMPT: Xây dựng "Stock Tools Platform" — tool đầu tiên là Radar Lệnh Lớn (VN100, realtime)

> Dùng prompt này cho một coding agent (ví dụ Claude Code) chạy trong một repo trống.
> Ngôn ngữ làm việc với người dùng: tiếng Việt. Code, tên biến, comment, tài liệu kỹ thuật: tiếng Anh. Chuỗi hiển thị cho người dùng cuối (cảnh báo, dashboard): tiếng Việt.

---

## 1. Vai trò và mục tiêu

Bạn là kỹ sư phần mềm senior kiêm kiến trúc sư hệ thống dữ liệu realtime. Nhiệm vụ: thiết kế và xây dựng một **nền tảng gồm nhiều công cụ (tool) phân tích thị trường chứng khoán Việt Nam**, trong đó **tool đầu tiên là Radar Lệnh Lớn** quét các giao dịch lớn của rổ **VN100** theo thời gian thực.

Điều quan trọng nhất: **nền tảng phải mở rộng được**. Radar chỉ là tool số 1; sau này sẽ thêm nhiều tool khác (theo dõi khối ngoại, quét phân kỳ MACD, đột biến khối lượng, theo dõi sự kiện doanh nghiệp, v.v.). Mọi quyết định thiết kế phải được đánh giá theo câu hỏi: "thêm tool thứ hai, thứ ba có phải sửa lõi không?". Đáp án mong muốn: không.

Hệ thống **chỉ đọc dữ liệu và phân tích**. Tuyệt đối không có chức năng đặt lệnh, chuyển tiền hay chạm vào tài khoản giao dịch. Giao diện phải có dòng lưu ý: thông tin chỉ mang tính tham khảo, không phải khuyến nghị đầu tư.

## 2. Các quyết định đã chốt (không hỏi lại)

- Phạm vi quét: **rổ VN100**. Danh sách mã lấy từ cấu hình/nguồn dữ liệu, **không hardcode**; thành phần rổ đổi định kỳ nên phải có cơ chế nạp lại.
- Yêu cầu: **phát hiện theo thời gian thực** (stream xử lý từng tick). Độ trễ vài giây là chấp nhận được; không cần tối ưu micro-giây.
- Hệ thống là **read-only**, một người dùng (single-tenant) ở giai đoạn đầu, nhưng thiết kế không được chặn việc mở rộng đa người dùng sau này.
- Tool radar phải dùng dữ liệu **từng giao dịch (tick)** từ nhà cung cấp có WebSocket realtime. Dữ liệu tổng hợp theo phiên là không đủ.
- Người dùng chạy hệ thống trên máy cá nhân hoặc một VPS nhỏ; ưu tiên đơn giản vận hành (Docker Compose), tránh hạ tầng nặng khi chưa cần.

## 3. Nguyên tắc làm việc bắt buộc

1. **Không bịa API.** Mọi tên endpoint, tên trường, định dạng message của nhà cung cấp (DNSE, SSI, VNDirect...) phải được **đọc từ tài liệu chính thức hoặc mẫu phản hồi thật**. Nếu chưa xác minh được, ghi `TODO(verify)` trong code, ghi vào `docs/open-questions.md`, và viết adapter theo một interface trừu tượng kèm dữ liệu giả. Không đoán.
2. **Plan trước, code sau.** Giai đoạn đầu tiên chỉ sinh tài liệu thiết kế và dừng để người dùng duyệt (xem mục 12).
3. **Mọi thứ phải kiểm thử được khi thị trường đóng cửa.** Có nhà cung cấp `ReplayProvider` phát lại tick đã ghi và `SyntheticProvider` sinh dữ liệu giả có kiểm soát. Toàn bộ pipeline chạy được và cho kết quả **tất định** trên dữ liệu replay.
4. **Ghi lại quyết định kiến trúc** bằng ADR ngắn trong `docs/adr/`. Mỗi ADR: bối cảnh, lựa chọn, lý do, hệ quả.
5. **Hỏi người dùng chỉ khi bị chặn thật sự** (thiếu tài khoản, thiếu khóa, điều khoản không rõ). Với mọi chuyện khác, chọn mặc định hợp lý, nêu rõ giả định, đi tiếp.
6. **Bảo mật cơ bản:** khóa API và token chỉ qua biến môi trường hoặc secret manager, có `.env.example`, tuyệt đối không commit hoặc ghi log giá trị bí mật.
7. **Tôn trọng điều khoản và giới hạn truy cập** của từng nhà cung cấp: giới hạn số mã đăng ký, tần suất gọi, quy định lưu trữ và phân phối lại dữ liệu. Nếu điều khoản chưa rõ, ghi vào câu hỏi mở thay vì mặc định là được.
8. Mỗi giai đoạn kết thúc bằng một báo cáo ngắn: đã làm gì, cách chạy thử, giả định, việc còn lại, rủi ro.

## 4. Kiến trúc tổng thể

Chia làm hai tầng: **Core (lõi dùng chung)** và **Tools (plugin)**.

```
Provider adapters (WebSocket/REST)      Context providers (REST/MCP)
        |                                        |
        v                                        v
  Ingest & Normalize ---> Event Bus <--- Context/Baseline Service
                              |
                +-------------+-------------+
                |             |             |
           Tool: Radar    Tool: B       Tool: C   (plugin, cô lập trạng thái)
                |             |             |
                +------> Signal Bus <-------+
                              |
              +---------------+----------------+
              |               |                |
        Alert Router     Storage (async)   API / WebSocket
        (Telegram,                              |
         Dashboard)                          Dashboard
```

### Thành phần lõi (Core)

- **Provider adapters:** mỗi nhà cung cấp dữ liệu thực thi chung một interface. Việc thêm nhà cung cấp mới chỉ là thêm một adapter.
- **Ingest và chuẩn hóa:** nhận message thô, kiểm tra hợp lệ, loại trùng, phát hiện thiếu gói, gắn thời gian nhận, chuyển sang mô hình sự kiện chuẩn (mục 7).
- **Event Bus:** phân phối sự kiện thị trường đến các tool theo đăng ký (subscription). Mặc định Redis Streams; lớp bọc cho phép đổi sang cách khác và có bản in-memory cho test.
- **Context/Baseline Service:** cung cấp dữ liệu nền không realtime cho tool (lịch sử khối lượng, kích thước giao dịch điển hình, khối ngoại, sự kiện doanh nghiệp, lịch giao dịch). Nguồn: REST của nhà cung cấp hoặc MCP; tách khỏi đường nóng.
- **Signal Bus và Alert Router:** nhận `Signal` từ các tool, dedupe, cooldown, giới hạn tần suất, rồi gửi qua kênh (Telegram, dashboard). Người dùng bật/tắt theo từng tool, từng mức độ.
- **Storage:** ghi tick, tín hiệu, trạng thái bất đồng bộ (không chặn đường phát hiện). Mặc định PostgreSQL + TimescaleDB.
- **Scheduler:** tác vụ định kỳ (làm nóng baseline trước phiên, báo cáo cuối phiên, dọn dữ liệu). Nhận biết lịch giao dịch và ngày lễ.
- **API/WebSocket:** REST cho truy vấn lịch sử và cấu hình, WebSocket đẩy tín hiệu realtime tới dashboard.
- **Config và Registry:** cấu hình theo file YAML + biến môi trường (Pydantic Settings). Registry khám phá và nạp tool.
- **Observability:** log có cấu trúc, metric (độ trễ tick tới tín hiệu, số tick/giây, độ trễ feed, số lần reconnect), endpoint `/health` và `/ready`.

### Hợp đồng plugin (Tool contract)

Mỗi tool là một gói độc lập. Phác thảo interface (có thể điều chỉnh, giữ tinh thần):

```python
class Tool(Protocol):
    id: str                      # "large_order_radar"
    version: str

    def subscriptions(self) -> list[Subscription]:
        """Tool cần luồng sự kiện nào, cho mã nào (vd: tick của universe VN100)."""

    async def on_start(self, ctx: ToolContext) -> None:
        """Nạp baseline, khôi phục trạng thái từ snapshot."""

    async def on_event(self, event: MarketEvent, ctx: ToolContext) -> list[Signal]:
        """Xử lý một sự kiện. Phải nhanh, không I/O chặn."""

    async def on_timer(self, now: datetime, ctx: ToolContext) -> list[Signal]:
        """Tác vụ định kỳ: đóng nhóm lệnh nhàn rỗi, tổng kết..."""

    def snapshot(self) -> bytes: ...
    def restore(self, data: bytes) -> None: ...

    async def on_stop(self) -> None: ...
```

Quy tắc plugin:
- Tool **không** gọi trực tiếp nhà cung cấp dữ liệu; chỉ nhận sự kiện từ bus và dữ liệu nền qua `ToolContext`.
- Mỗi tool có không gian trạng thái và cấu hình riêng, lỗi của một tool **không được làm sập** tool khác hoặc lõi (cách ly bằng bắt ngoại lệ, giới hạn thời gian xử lý, circuit breaker).
- Tool khai báo manifest (id, phiên bản, luồng dữ liệu cần, schema cấu hình, schema tín hiệu phát ra) để lõi tự kiểm tra và dashboard tự dựng màn hình cấu hình.
- Tín hiệu phát ra theo schema chung (mục 7) cộng thêm trường `features` tự do theo tool.

## 5. Nguồn dữ liệu

### Đường realtime (đường nóng)
- Interface `MarketDataProvider`: `connect()`, `subscribe(symbols, channels)`, `events() -> AsyncIterator[MarketEvent]`, `close()`.
- Hiện thực theo thứ tự: `SyntheticProvider`, `ReplayProvider`, sau đó adapter thật. Ứng viên adapter thật là **DNSE OpenAPI** (WebSocket có tick, giá dự kiến ATO/ATC, khối ngoại) và **SSI FastConnect Data** (realtime tick, sổ lệnh). Người dùng sẽ chọn nhà cung cấp theo tài khoản họ có; viết adapter cho nhà cung cấp họ xác nhận trước, các adapter còn lại chỉ cần khung.
- Trường cần xác minh từ tài liệu thật (không được đoán): có cờ mua/bán chủ động không, đơn vị khối lượng và giá, múi giờ và định dạng thời gian, mã phiên giao dịch, cách phân biệt khớp lệnh thường với thỏa thuận, giới hạn số mã đăng ký đồng thời, cơ chế xác thực và gia hạn token, cơ chế heartbeat/reconnect.

### Đường nền (không realtime)
- Interface `ContextProvider` cung cấp: lịch sử giá/khối lượng theo mã, thống kê kích thước giao dịch, khối ngoại, giao dịch thỏa thuận, sự kiện doanh nghiệp, thành phần rổ, lịch giao dịch.
- Nguồn ứng viên: **VNDirect** (người dùng đã kết nối được MCP của VNDirect; bộ tool này có giá mới nhất, giá theo ngày, khối ngoại, sự kiện công ty, lịch giao dịch, chỉ số tài chính). Một dự án mã nguồn mở cộng đồng dùng các API công khai của VNDirect (`dchart-api` cho nến, `api-finfo` cho chỉ số cơ bản); đây là thông tin bên thứ ba, **phải xác minh điều khoản sử dụng và độ ổn định** trước khi phụ thuộc.
- Lưu ý: MCP sinh ra để Claude gọi, không phù hợp làm đường nóng. Nếu dùng làm Context Provider, đặt sau cache và chạy ngoài đường nóng (làm nóng trước phiên, cập nhật định kỳ). Nếu không gọi MCP được từ backend, dùng REST hoặc nguồn khác thay thế; ghi quyết định bằng ADR.
- Mọi Context Provider phải có cache (TTL), retry có backoff và chế độ suy giảm: nếu nguồn nền lỗi, tool vẫn chạy với ngưỡng tuyệt đối và đánh dấu `baseline_cold`.

## 6. Tool #1: Radar Lệnh Lớn

### Mục tiêu
Phát hiện và cảnh báo, gần thời gian thực, các **dòng lệnh lớn** (một giao dịch lớn hoặc một chuỗi giao dịch cùng chiều bị chia nhỏ) trên các mã VN100, kèm bối cảnh để người dùng hiểu ý nghĩa.

### Dữ liệu vào
Luồng tick khớp lệnh của universe VN100, luồng giá dự kiến ATO/ATC nếu có, và dữ liệu nền (baseline) từ Context Provider.

### Xử lý theo từng tick
1. **Chuẩn hóa và phân loại chiều.** Dùng cờ mua/bán chủ động của nhà cung cấp nếu có. Nếu không, suy ra theo quote rule / tick rule (Lee-Ready) dựa trên giá chào mua/bán tốt nhất. Luôn lưu `side_source` (`provider` | `quote_rule` | `tick_rule` | `unknown`) và coi chiều suy luận là xấp xỉ có độ tin cậy thấp hơn, đặc biệt với giao dịch nằm trong spread.
2. **Hai loại ngưỡng "lệnh lớn", cấu hình được:**
   - **Ngưỡng tuyệt đối:** giá trị giao dịch ≥ X đồng (mặc định tham khảo 500 triệu, giống định nghĩa công khai của một số công cụ thị trường; chỉ là giá trị khởi đầu, phải cấu hình được).
   - **Ngưỡng tương đối theo mã:** giá trị giao dịch ≥ k lần giá trị giao dịch điển hình của mã đó (trung vị hoặc phân vị cao trong N phiên gần đây), hoặc vượt phân vị P của chính mã. Nhóm mã theo mức thanh khoản để dùng tham số riêng.
   - Một giao dịch là "lớn" nếu thỏa ít nhất một ngưỡng; ghi nhận ngưỡng nào kích hoạt.
3. **Gom lệnh mẹ (parent order grouping).** Gom các giao dịch cùng mã, cùng chiều, giá nằm trong dải cho phép (tính theo bước giá, lấy từ cấu hình và xác minh theo quy định sàn hiện hành), cách nhau không quá G giây. Nhóm có vòng đời `OPEN → UPDATE → CLOSE`:
   - `OPEN` khi tổng giá trị của nhóm vượt ngưỡng nhóm.
   - `UPDATE` khi tổng giá trị tăng thêm một bước đáng kể (ví dụ +50%) để không spam.
   - `CLOSE` sau một khoảng nhàn rỗi, hoặc khi đổi phiên.
4. **Chỉ số theo mã (cửa sổ trượt, cập nhật online):** tổng giá trị mua/bán lớn, **ròng lớn**, tỷ lệ lớn trên tổng giá trị chủ động, tốc độ (giá trị/phút), mức tác động giá.
5. **Tín hiệu hấp thụ (absorption), phiên bản sau:** ròng lớn rất cao nhưng giá gần như không dịch chuyển trong cùng cửa sổ. Đặt sau cờ cấu hình, tắt mặc định ở bản đầu.
6. **Điểm radar và mức độ.** Gộp các yếu tố (giá trị, độ lớn tương đối, tốc độ, độ tập trung, tác động giá, độ tin cậy của chiều, xác nhận từ khối ngoại hoặc thỏa thuận nếu có) thành điểm số và mức độ (`info`, `notice`, `alert`). Trọng số và ngưỡng cấu hình được; mọi tín hiệu phải kèm `explanation` ghi rõ yếu tố nào đóng góp bao nhiêu để có thể kiểm chứng và chỉnh.

### Nhận biết phiên giao dịch
Hành vi khác nhau theo phiên: ATO, khớp liên tục buổi sáng, nghỉ trưa, khớp liên tục buổi chiều, ATC, thỏa thuận. Detector phải biết đang ở phiên nào:
- Lịch phiên là **cấu hình**, và bạn phải **xác minh giờ giấc theo quy định hiện hành của sàn** thay vì dùng số cũ trong trí nhớ.
- Trong ATO/ATC lệnh khớp theo một giá, nên dùng luồng giá dự kiến nếu có; không áp dụng logic gom lệnh của phiên liên tục một cách máy móc.
- Giao dịch thỏa thuận xử lý và hiển thị **riêng**, không trộn vào chỉ số khớp lệnh.

### Làm nóng baseline và khởi động nguội
- Trước giờ mở cửa, nạp baseline cho cả VN100 từ Context Provider (tác vụ Scheduler).
- Nếu thiếu baseline, dùng ngưỡng tuyệt đối và đánh dấu tín hiệu `baseline_cold` (hiển thị cảnh báo độ tin cậy thấp hơn).
- Hỗ trợ khôi phục trạng thái (snapshot) sau khi khởi động lại giữa phiên để không mất các nhóm lệnh đang mở.

### Chống nhiễu cho cảnh báo
Dedupe theo `dedupe_key`, cooldown theo mã và chiều, gộp nhiều tín hiệu cùng mã trong cửa sổ ngắn thành một tin cập nhật, giới hạn số cảnh báo tối đa mỗi phút trên mỗi kênh, và có bản tổng kết cuối phiên.

## 7. Mô hình dữ liệu chuẩn (định nghĩa bằng Pydantic v2)

- `Instrument`: symbol, exchange, tick_size_rule, lot_size, sector.
- `Tick` (MarketEvent): provider, symbol, ts_exchange, ts_received, price, volume, value, side (`buy`|`sell`|`unknown`), side_source, session, is_negotiated (thỏa thuận), seq (nếu có), raw_ref.
- `Quote` (MarketEvent): symbol, ts, best_bid, best_ask, bid_size, ask_size (nếu có).
- `ExpectedPrice` (MarketEvent): symbol, ts, session (ATO/ATC), price, volume (nếu có).
- `ForeignFlow` (MarketEvent hoặc dữ liệu nền): symbol, ts, buy/sell value, net value.
- `Signal`:
  - `signal_id`, `tool_id`, `tool_version`, `symbol`, `ts`
  - `kind` (ví dụ `block_order_open`, `block_order_update`, `block_order_close`)
  - `side`, `score`, `severity`, `state`
  - `features` (dict theo tool), `explanation` (danh sách yếu tố đóng góp), `quality_flags` (ví dụ `baseline_cold`, `side_inferred`)
  - `dedupe_key`, `correlation_id`
- `Alert`: signal_id, channel, rendered_text, sent_at, status.
- Mọi dấu thời gian lưu **UTC** kèm múi giờ gốc; hiển thị theo múi giờ Việt Nam.

## 8. Cảnh báo và giao diện

- **Telegram:** tin cảnh báo ngắn gọn bằng tiếng Việt (mã, chiều, tổng giá trị, số lệnh trong nhóm, so với mức bình thường, các cờ chất lượng, thời điểm). Có lệnh đơn giản để bật/tắt tool, đổi mức độ tối thiểu, xem trạng thái.
- **Dashboard web (React + Vite + TypeScript, giữ tối giản):**
  - Luồng tín hiệu realtime qua WebSocket, lọc theo mã/chiều/mức độ.
  - Bảng radar VN100 xếp theo ròng lớn và điểm radar.
  - Trang chi tiết mã: dòng tín hiệu, biểu đồ giá/khối lượng với đánh dấu lệnh lớn, các cờ chất lượng.
  - Trang cấu hình tool (dựng từ manifest), trang sức khỏe hệ thống (độ trễ, trạng thái kết nối feed).
  - Dòng lưu ý: không phải khuyến nghị đầu tư.

## 9. Yêu cầu phi chức năng

- **Độ trễ:** đo và hiển thị độ trễ end-to-end (tick nhận → tín hiệu phát). Mục tiêu khởi điểm: vài giây. Độ trễ của chính feed nhà cung cấp phải được tách riêng khỏi độ trễ nội bộ.
- **Độ bền:** tự kết nối lại có backoff, theo dõi heartbeat, phát hiện mất gói và tick muộn/lặp, backpressure khi đông tick, không mất tín hiệu khi tool lỗi tạm thời.
- **Cô lập lỗi:** một tool chậm hoặc ném ngoại lệ không được làm nghẽn bus hay các tool khác.
- **Khả năng quan sát:** log có cấu trúc kèm `correlation_id`, metric kiểu Prometheus, trang trạng thái.
- **Cấu hình:** mọi ngưỡng và tham số nằm trong cấu hình, có giá trị mặc định, validate khi khởi động, hỗ trợ nạp lại nóng nếu hợp lý.
- **Dữ liệu:** chính sách lưu và dọn tick (ví dụ giữ chi tiết N ngày, sau đó chỉ giữ tổng hợp), tuân thủ điều khoản nhà cung cấp.
- **Chất lượng mã:** type hint đầy đủ, `ruff` và `mypy` sạch, kiểm thử tự động, CI cơ bản.

## 10. Công nghệ mặc định (được phép đề xuất thay thế kèm ADR giải thích)

- Python 3.12, `asyncio`, FastAPI, Pydantic v2, SQLAlchemy 2 + `asyncpg`, Alembic.
- Redis (Streams cho event bus, cache), PostgreSQL + TimescaleDB.
- `uv` hoặc `poetry` quản lý phụ thuộc, `pytest` + `pytest-asyncio` + `hypothesis` cho property test.
- Docker Compose cho môi trường phát triển và chạy thật; một lệnh `make up` hoặc tương đương.
- Frontend: React + Vite + TypeScript, WebSocket gốc.
- Tùy chọn giai đoạn sau: lớp "giải thích tín hiệu" dùng LLM (gọi API Claude) để soạn lời bình ngắn từ `features` và `explanation` của tín hiệu. Lớp này **không được nằm trên đường nóng** và phải tắt được; mọi quyết định phát hiện vẫn là logic xác định.

## 11. Chiến lược kiểm thử

- **Kiểm thử đơn vị:** phân loại chiều, ngưỡng, gom lệnh mẹ (có property test: tổng giá trị nhóm = tổng giá trị thành phần, thứ tự đến không làm sai kết quả trong dung sai cho phép), cooldown/dedupe, nhận biết phiên.
- **Kiểm thử replay tất định:** bộ tick ghi sẵn (hoặc sinh giả có seed) chạy qua toàn pipeline, so khớp tín hiệu với kết quả mong đợi (golden file). Cần ít nhất các kịch bản: một lệnh lớn đơn, lệnh lớn chia nhỏ, đảo chiều giữa chừng, chuỗi bị ngắt bởi nghỉ trưa, ATO/ATC, thỏa thuận xen kẽ, tick trùng và muộn.
- **Kiểm thử chịu tải:** phát lại tick với tốc độ nhanh gấp nhiều lần thực tế, đo độ trễ và mức dùng bộ nhớ.
- **Kiểm thử cô lập lỗi:** chèn một tool cố tình lỗi/chậm, xác nhận tool khác không bị ảnh hưởng.
- **Kịch bản kết nối:** mô phỏng đứt kết nối, mất gói, token hết hạn.
- **Backtest nhẹ:** script chạy radar trên dữ liệu lịch sử đã lưu và xuất báo cáo (số tín hiệu theo mã/giờ, phân bố điểm) để chỉnh ngưỡng. Không tuyên bố hiệu quả dự báo giá.

## 12. Lộ trình theo giai đoạn (kèm tiêu chí hoàn thành)

**Giai đoạn 0 — Khám phá và thiết kế (DỪNG để người dùng duyệt)**
- Đọc tài liệu chính thức của các nhà cung cấp ứng viên; lập `docs/provider-notes.md` liệt kê các trường đã **xác minh** và chưa xác minh.
- Sinh `docs/architecture.md`, các ADR chính (event bus, lưu trữ, mô hình plugin, nguồn baseline), `docs/open-questions.md`, và bản ước tính rủi ro.
- Tiêu chí hoàn thành: người dùng duyệt thiết kế và trả lời các câu hỏi chặn (nhà cung cấp nào, có khóa chưa).

**Giai đoạn 1 — Khung nền tảng**
- Cấu trúc repo, Docker Compose, cấu hình, log, `/health`, CI.
- Mô hình dữ liệu, Event Bus (Redis + in-memory), Tool registry và hợp đồng plugin, `SyntheticProvider` và `ReplayProvider`, ghi tick vào storage.
- Một **tool mẫu tầm thường** (ví dụ đếm tick theo mã) chứng minh ranh giới plugin hoạt động.
- Tiêu chí: `make up` chạy được, tool mẫu nhận tick giả và phát tín hiệu, test xanh.

**Giai đoạn 2 — Radar lõi trên dữ liệu replay**
- Phân loại chiều, hai loại ngưỡng, gom lệnh mẹ, chỉ số theo mã, điểm radar, nhận biết phiên, dedupe/cooldown, snapshot/restore.
- Bộ kịch bản replay và golden test (mục 11), script backtest nhẹ.
- Tiêu chí: toàn bộ kịch bản pass, kết quả tất định, báo cáo backtest chạy được.

**Giai đoạn 3 — Kết nối nhà cung cấp thật + VN100**
- Adapter cho nhà cung cấp người dùng chọn, nạp universe VN100, đăng ký theo giới hạn của nhà cung cấp, reconnect/heartbeat, đo độ trễ feed, ghi tick thật để dùng cho replay sau này.
- Tiêu chí: chạy suốt một phiên thật không sập, tick thật đi qua radar, số liệu độ trễ hiển thị được. Ghi chú mọi điểm khác với giả định ban đầu.

**Giai đoạn 4 — Cảnh báo và dashboard**
- Alert Router, Telegram, API/WebSocket, dashboard theo mục 8.
- Tiêu chí: tín hiệu thật xuất hiện trên Telegram và dashboard, có chống spam, người dùng bật/tắt được từng tool.

**Giai đoạn 5 — Bối cảnh và tinh chỉnh**
- Context Provider (VNDirect hoặc nguồn khác đã chốt), làm nóng baseline trước phiên, làm giàu tín hiệu bằng khối ngoại/thỏa thuận/sự kiện doanh nghiệp, tinh chỉnh trọng số điểm radar bằng dữ liệu đã ghi.
- Tiêu chí: tỷ lệ tín hiệu `baseline_cold` giảm rõ rệt, giải thích tín hiệu thể hiện được yếu tố đóng góp.

**Giai đoạn 6 — Chứng minh khả năng mở rộng**
- Thêm **tool thứ hai** hoàn toàn mới (đề xuất: radar đột biến khối lượng theo phút hoặc theo dõi khối ngoại) mà **không sửa lõi** (chỉ được thêm, không đổi hợp đồng). Nếu buộc phải sửa lõi, ghi lại lý do trong ADR.
- Viết `docs/writing-a-tool.md` hướng dẫn thêm tool mới.
- Tiêu chí: tool thứ hai chạy song song với radar, lỗi của một tool không ảnh hưởng tool kia.

## 13. Ngoài phạm vi (không làm)

- Đặt lệnh, kết nối tài khoản giao dịch, bất kỳ thao tác tài chính nào.
- Dự báo giá hoặc khuyến nghị mua/bán; chỉ phát hiện và mô tả dòng lệnh.
- Hạ tầng Kubernetes, microservice tách rời quá sớm.
- Ứng dụng di động.
- Học máy phức tạp trong phát hiện ở giai đoạn đầu (có thể bàn sau khi có đủ dữ liệu đã ghi).

## 14. Điều bạn phải xác minh hoặc hỏi (đưa vào `docs/open-questions.md`)

1. Nhà cung cấp tick nào người dùng có tài khoản (DNSE, SSI hay khác), gói nào, giới hạn số mã đăng ký, tần suất và điều khoản lưu/phân phối dữ liệu.
2. Feed có cờ mua/bán chủ động hay không; nếu không, độ chính xác chấp nhận được của việc suy luận chiều.
3. Giờ giấc các phiên giao dịch hiện hành và quy tắc bước giá theo sàn; lịch nghỉ lễ.
4. Cách lấy danh sách thành phần VN100 chính thức và cập nhật khi đổi rổ.
5. Độ ổn định, điều khoản và giới hạn của nguồn dữ liệu nền (VNDirect MCP/REST); có gọi được MCP từ backend hay không.
6. Nơi chạy hệ thống (máy cá nhân hay VPS) và yêu cầu uptime thực tế.
7. Kênh nhận cảnh báo ưu tiên và có cần đa người dùng sớm không.

## 15. Cách báo cáo với người dùng

- Bắt đầu mỗi giai đoạn bằng một câu nêu bạn sắp làm gì; kết thúc bằng báo cáo ngắn theo mục 3.8.
- Khi thấy thiết kế ở trên mâu thuẫn với thực tế dữ liệu (ví dụ feed không có trường cần thiết), **dừng, nêu rõ phát hiện và đề xuất phương án**, đừng lặng lẽ đổi hướng.
- Luôn nêu rõ điều gì đã được kiểm chứng bằng dữ liệu thật và điều gì mới chỉ chạy trên dữ liệu giả.
