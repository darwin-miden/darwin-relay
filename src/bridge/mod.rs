//! Bridge abstraction. `BridgeClient` is the trait the relay service
//! calls to push USDC from the ETH-side escrow into the Darwin
//! operator's Miden wallet.
//!
//! Two implementations:
//!
//! - [`MockBridge`]: in-memory, instant. For dev + integration tests
//!   while we wait for the canonical AggLayer bridge to be public on
//!   Miden testnet.
//! - `AggLayerBridge` (gated behind `miden-live` feature, future): real
//!   bridge-out / claim flow against the canonical Miden ↔ Ethereum
//!   bridge.

mod mock;

pub use mock::MockBridge;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BridgeReceipt {
    /// Identifier the bridge issues for this transfer (an L1 tx hash,
    /// an AggLayer leaf index, or a mock counter — depends on impl).
    pub bridge_ref: String,
    pub status: BridgeStatus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BridgeStatus {
    Pending,
    Confirmed,
    Failed,
}

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("bridge transient: {0}")]
    Transient(String),
    #[error("bridge permanent: {0}")]
    Permanent(String),
}

#[async_trait]
pub trait BridgeClient: Send + Sync {
    /// Initiate a transfer of `amount` units of USDC from the
    /// escrow contract on ETH to the Miden relay wallet. Returns
    /// the bridge's identifier and an initial status.
    async fn bridge_out(
        &self,
        deposit_id: u64,
        amount_usdc: u128,
    ) -> Result<BridgeReceipt, BridgeError>;

    /// Poll a previously-initiated bridge for an updated status.
    /// Idempotent.
    async fn poll(&self, bridge_ref: &str) -> Result<BridgeReceipt, BridgeError>;
}
