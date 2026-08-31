//! Injected round-trip time and request counting for the fake servers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::Response;

#[derive(Default)]
pub struct Net {
    rtt_ms: AtomicU64,
    log: std::sync::Mutex<Vec<String>>,
}

impl Net {
    pub fn set_rtt(&self, rtt: Duration) {
        self.rtt_ms.store(rtt.as_millis() as u64, Ordering::Relaxed);
    }

    /// `METHOD /path` of every request since the last call.
    pub fn take(&self) -> Vec<String> {
        std::mem::take(&mut self.log.lock().unwrap())
    }

    pub fn layer<S: Clone + Send + Sync + 'static>(
        self: &Arc<Self>,
        router: Router<S>,
    ) -> Router<S> {
        router.layer(from_fn_with_state(self.clone(), delay))
    }
}

async fn delay(State(net): State<Arc<Net>>, req: Request, next: Next) -> Response {
    net.log
        .lock()
        .unwrap()
        .push(format!("{} {}", req.method(), req.uri().path()));
    let rtt = net.rtt_ms.load(Ordering::Relaxed);
    if rtt > 0 {
        tokio::time::sleep(Duration::from_millis(rtt)).await;
    }
    next.run(req).await
}
