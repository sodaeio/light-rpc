//! Singleflight: concurrent identical requests share one backend fetch.
//! Bounded at MAX_INFLIGHT distinct keys; overflow bypasses uncoalesced.

use std::hash::Hash;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::OnceCell;

const MAX_INFLIGHT: usize = 4096;

pub struct Coalescer<K, V> {
    inflight: DashMap<K, Arc<OnceCell<Arc<V>>>>,
}

impl<K, V> Default for Coalescer<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self {
            inflight: DashMap::new(),
        }
    }
}

impl<K, V> Coalescer<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// First caller installs a OnceCell and runs `fetch`; concurrent callers
    /// await the cell and clone the Arc.
    #[allow(clippy::clone_on_ref_ptr)]
    pub async fn run<F, Fut>(&self, key: K, fetch: F) -> Arc<V>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = V>,
    {
        if self.inflight.len() >= MAX_INFLIGHT {
            return Arc::new(fetch().await);
        }

        let cell = self
            .inflight
            .entry(key.clone())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        let value = cell
            .get_or_init(|| async move { Arc::new(fetch().await) })
            .await
            .clone();

        self.inflight.remove(&key);
        value
    }
}
