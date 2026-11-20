//! OpenAPI description of the HTTP API, served as `/openapi.json`, with Swagger UI at
//! `/docs` (assets are vendored into the binary, so the page loads under the same CSP as
//! the dashboard). Turn both off with `api.docs_enabled: false`.
//!
//! The paths come from the `#[utoipa::path]` attributes on the handlers in `api/mod.rs`;
//! add new routes to `ApiDoc` below (a test checks every route is listed).

use axum::Router;
use serde::Serialize;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use super::AppState;

pub const SPEC_PATH: &str = "/openapi.json";
pub const UI_PATH: &str = "/docs";

#[derive(OpenApi)]
#[openapi(
    info(
        title = "astrq — Stock Tools Platform API",
        description = "Read-only realtime analysis of Vietnamese equities (VN100). \
                       There is no trading endpoint.\n\n\
                       Thông tin chỉ mang tính tham khảo, không phải khuyến nghị đầu tư.",
    ),
    paths(
        super::health,
        super::ready,
        super::metrics,
        super::status,
        super::universe,
        super::history,
        super::signals,
        super::alerts,
        super::summary,
        super::screener_meta,
        super::screener_run,
        super::tools,
        super::set_enabled,
        super::tool_view,
        super::ws,
    ),
    tags(
        (name = "ops", description = "Health, readiness, metrics"),
        (name = "platform", description = "Status, universe, available sessions"),
        (name = "signals", description = "Signals, alerts, daily summary"),
        (name = "tools", description = "Installed tools and their dashboard views"),
        (name = "screener", description = "On-demand indicator screener on daily bars"),
        (name = "realtime", description = "WebSocket push channel"),
    )
)]
pub struct ApiDoc;

/// Swagger UI + spec routes (merged into the main router when docs are enabled).
pub fn routes() -> Router<AppState> {
    SwaggerUi::new(UI_PATH)
        .url(SPEC_PATH, ApiDoc::openapi())
        .into()
}

// ---------------------------------------------------------------- doc-only shapes
// Handlers build these bodies with `json!`; the structs only describe them.

/// Error body of 4xx/5xx JSON responses.
#[derive(Serialize, ToSchema)]
pub struct ErrorDetail {
    #[schema(value_type = Object)]
    pub detail: serde_json::Value,
}

#[derive(Serialize, ToSchema)]
pub struct Health {
    #[schema(example = "ok")]
    pub status: String,
}

#[derive(Serialize, ToSchema)]
pub struct Ready {
    pub ready: bool,
}

#[derive(Serialize, ToSchema)]
pub struct Summary {
    pub text: Option<String>,
}

/// Sessions that can be viewed: the live one and past ones still on record.
#[derive(Serialize, ToSchema)]
pub struct History {
    /// Session shown when no `date` is given.
    #[schema(format = Date)]
    pub live_date: String,
    /// Today's trading date (Vietnam time).
    #[schema(format = Date)]
    pub today: String,
    pub retention_days: u32,
    /// Past sessions with data, usable as `date=` on `/api/signals` and views.
    pub dates: Vec<String>,
}

/// Query parameters of `/api/tools/{id}/view` (the radar's; other tools ignore what they
/// do not use). Unknown keys are dropped and numbers clamped.
#[derive(IntoParams)]
#[into_params(parameter_in = Query)]
#[allow(dead_code)]
pub struct ViewParams {
    /// Focus on one ticker of the universe.
    symbol: Option<String>,
    /// `1`: full leaderboard instead of the compact overview.
    full: Option<String>,
    /// Large orders listed per symbol (1–30).
    orders: Option<u32>,
    /// Orders that include their trades (0–30).
    orders_with_trades: Option<u32>,
    /// Trades listed per order (0–50).
    trades: Option<u32>,
    /// Timeline: view as of `HH:MM` (Vietnam time).
    at: Option<String>,
    /// `1`: include the sector heatmap.
    sectors: Option<String>,
    /// `1`: include market flow.
    flow: Option<String>,
    /// Flow bucket in minutes (with `flow=1`).
    bucket: Option<u32>,
    /// Past session `YYYY-MM-DD`; omitted = live session.
    #[param(format = Date)]
    date: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_lists_every_route() {
        let spec = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let paths = spec["paths"].as_object().unwrap();
        // keep in sync with `api::router`
        for p in [
            "/health",
            "/ready",
            "/metrics",
            "/api/status",
            "/api/history",
            "/api/screener/meta",
            "/api/screener/run",
            "/api/universe",
            "/api/signals",
            "/api/alerts",
            "/api/summary",
            "/api/tools",
            "/api/tools/{id}/enabled",
            "/api/tools/{id}/view",
            "/ws",
        ] {
            assert!(paths.contains_key(p), "{p} missing from the OpenAPI spec");
        }
        assert_eq!(paths.len(), 15, "a documented path is not routed");
        for s in [
            "Signal",
            "ScanRequest",
            "Condition",
            "Operand",
            "ErrorDetail",
        ] {
            assert!(
                spec["components"]["schemas"].get(s).is_some(),
                "schema {s} missing"
            );
        }
    }
}
