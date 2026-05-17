//! ETH event watcher. Subscribes to `RelayDepositRequested` logs on
//! `DarwinRelayDeposit.sol` and turns each one into a `DepositRecord`
//! inserted into the store.
//!
//! Two impls:
//!
//! - [`MockWatcher`]: in-memory channel for tests. The test harness
//!   pushes synthetic deposit events into the watcher's queue.
//! - [`AlloyWatcher`]: real `eth_subscribe("logs", filter)` over a
//!   WebSocket provider.
//!
//! The watcher exposes a `next()` method that returns the next
//! observed deposit (with a `pop` semantic). The driver loop calls
//! `next()` repeatedly and inserts each result into `DepositStore`.

mod alloy_impl;
mod mock;

pub use alloy_impl::AlloyWatcher;
pub use mock::MockWatcher;

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum WatcherError {
    #[error("watcher transient: {0}")]
    Transient(String),
    #[error("watcher permanent: {0}")]
    Permanent(String),
    #[error("watcher closed")]
    Closed,
}

/// One observed RelayDepositRequested log, decoded to the relay's
/// `DepositRecord` shape so insertion into the store is trivial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedDeposit {
    pub id: u64,
    pub user_eth: String,
    pub basket_id: String,
    pub miden_recipient: String,
    pub amount_usdc: u128,
    pub requested_at_unix: i64,
}

#[async_trait]
pub trait DepositWatcher: Send + Sync {
    /// Block until the next deposit event arrives. Returns
    /// `WatcherError::Closed` when the underlying stream ends.
    async fn next(&mut self) -> Result<ObservedDeposit, WatcherError>;
}
