//! HTTP server loop. `axum::serve` sets no header-read timeout and no connection limit,
//! so a client could hold sockets open forever by sending headers slowly (slow-loris).
//! This loop adds both, passes the peer address to handlers (`ConnectInfo`), supports
//! WebSocket upgrades and shuts down gracefully.

use axum::extract::ConnectInfo;
use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tower::ServiceExt;

use crate::core::config::ApiLimits;
use crate::core::metrics::METRICS;

pub async fn serve(
    listener: TcpListener,
    app: Router,
    limits: &ApiLimits,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let conns = Arc::new(Semaphore::new(limits.max_connections.max(1)));
    let header_timeout = Duration::from_secs_f64(limits.header_timeout_sec.max(1.0));
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    tokio::pin!(shutdown);
    loop {
        let (stream, peer) = tokio::select! {
            r = listener.accept() => match r {
                Ok(x) => x,
                Err(e) => {
                    // e.g. EMFILE under a connection flood: back off instead of spinning
                    tracing::warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            _ = &mut shutdown => break,
        };
        let Ok(permit) = conns.clone().try_acquire_owned() else {
            METRICS.inc("http_rejected_total", 1.0, &[("reason", "connections")]);
            drop(stream); // over capacity: close at once, costs nothing
            continue;
        };
        let _ = stream.set_nodelay(true);
        let svc = app
            .clone()
            .map_request(move |req: hyper::Request<hyper::body::Incoming>| {
                let mut req = req.map(axum::body::Body::new);
                req.extensions_mut().insert(ConnectInfo(peer));
                req
            });
        let svc = TowerToHyperService::new(svc);
        let mut builder = Builder::new(TokioExecutor::new());
        builder
            .http1()
            .timer(TokioTimer::new())
            .header_read_timeout(header_timeout)
            .keep_alive(true);
        builder
            .http2()
            .timer(TokioTimer::new())
            .max_concurrent_streams(64)
            .keep_alive_interval(Some(Duration::from_secs(30)))
            .keep_alive_timeout(Duration::from_secs(20));
        let conn = builder
            .serve_connection_with_upgrades(TokioIo::new(stream), svc)
            .into_owned();
        let conn = graceful.watch(conn);
        tokio::spawn(async move {
            let _ = conn.await; // client errors / resets are routine
            drop(permit);
        });
    }
    drop(listener);
    // let in-flight requests finish (WebSockets are closed by the shutdown)
    tokio::select! {
        _ = graceful.shutdown() => {},
        _ = tokio::time::sleep(Duration::from_secs(5)) => {},
    }
    Ok(())
}

/// Serve on an already-bound listener with default limits (tests, tools).
pub async fn serve_default(listener: TcpListener, app: Router) -> std::io::Result<()> {
    serve(listener, app, &ApiLimits::default(), std::future::pending()).await
}
