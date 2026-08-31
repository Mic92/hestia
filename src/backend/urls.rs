//! Signed download URLs of the Actions cache, resolved once per key:
//! concurrent readers of one pack share the lookup.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::OnceCell;

use super::Error;

type Cell = Arc<OnceCell<(Option<String>, Instant)>>;

pub struct UrlCache {
    ttl: Duration,
    cells: Mutex<HashMap<String, Cell>>,
}

impl UrlCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            cells: Mutex::default(),
        }
    }

    /// The cached value, or what `resolve` yields. `None` is not cached.
    pub async fn get<F>(&self, key: &str, force: bool, resolve: F) -> Result<Option<String>, Error>
    where
        F: Future<Output = Result<Option<String>, Error>>,
    {
        let cell = {
            let mut cells = self.cells.lock().unwrap();
            cells.retain(|_, c| {
                c.get()
                    .is_none_or(|(v, t)| v.is_some() && t.elapsed() < self.ttl)
            });
            if force {
                cells.remove(key);
            }
            cells.entry(key.to_owned()).or_default().clone()
        };
        let (v, _) = cell
            .get_or_try_init(|| async { resolve.await.map(|v| (v, Instant::now())) })
            .await?;
        Ok(v.clone())
    }

    pub fn evict(&self, key: &str) {
        self.cells.lock().unwrap().remove(key);
    }
}
