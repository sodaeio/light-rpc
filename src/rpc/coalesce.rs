//! Singleflight: collapses concurrent identical requests into one backend
//! fetch. Hot keys (USDC mint info, well-known program account info) get
//! hammered by every wallet on the network; with this in front of storage,
//! N concurrent callers wait on one fetch and all receive a clone of the
//! same Arc.
//!
//! Keys are method-specific (e.g. `("gAI", pubkey, encoding)`); the value is
//! whatever the handler returns (typically `serde_json::Value`).
//!
//! The deduplicator is intentionally bounded: at most `MAX_INFLIGHT` distinct
//! in-flight keys at once, after which new keys bypass and run uncoalesced.
//! This is fine — coalescing is opportunistic.

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

    /// Run `fetch` once per concurrent set of identical `key`s.
    ///
    /// First caller installs an OnceCell, drives the future, and stores the
    /// result. Concurrent callers see the OnceCell, await it, and clone the
    /// resulting Arc.
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

        // Best-effort eviction so the map doesn't grow unbounded across
        // distinct keys. The next caller for this key just installs a fresh
        // cell, which is correct.
        self.inflight.remove(&key);
        value
    }
}
