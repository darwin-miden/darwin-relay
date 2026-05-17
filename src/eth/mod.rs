//! ETH-side write surface for the relay. The service drives the
//! deposit FSM through this trait, and the trait has two impls:
//!
//! - [`MockEthClient`]: in-memory call recorder for unit tests.
//! - [`AlloyEthClient`]: real wire to a JSON-RPC provider via alloy
//!   1.x. Used in dev (anvil) and prod (Sepolia, mainnet).
//!
//! Every call returns a tx hash string (`0x…`) which the service
//! persists in SQLite so a restart can recover the in-flight state.

mod alloy_impl;
mod mock;

pub use alloy_impl::{connect_http_alloy_eth_client, AlloyEthClient, BasketRegistry};
pub use mock::{MockCall, MockEthClient};

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum EthError {
    #[error("eth transient: {0}")]
    Transient(String),
    #[error("eth permanent: {0}")]
    Permanent(String),
}

#[async_trait]
pub trait EthClient: Send + Sync {
    /// Call `DarwinRelayDeposit.claimDeposit(id)` from the operator
    /// address. Returns the tx hash.
    async fn claim_deposit(&self, deposit_id: u64) -> Result<String, EthError>;

    /// Call `DarwinRelayDeposit.confirmDeposit(id, basketAmount)`.
    /// Returns the tx hash. Idempotent on the contract side
    /// (re-calling on Settled reverts; we record the same tx hash).
    async fn confirm_deposit(
        &self,
        deposit_id: u64,
        basket_amount: u128,
    ) -> Result<String, EthError>;

    /// Call `DarwinRelayDeposit.refundDeposit(id, reason)`. Returns
    /// the tx hash.
    async fn refund_deposit(&self, deposit_id: u64, reason: &str) -> Result<String, EthError>;

    /// Call `DarwinBasketToken.mintTo(user, amount)` on the basket-
    /// specific token contract resolved from `basket_id`. Returns the
    /// tx hash.
    async fn mint_basket_to(
        &self,
        basket_id: &str,
        user: &str,
        amount: u128,
    ) -> Result<String, EthError>;
}
