//! Miden-side submitter. The `BridgedToMiden → MidenMinted` leg of
//! the deposit FSM.
//!
//! Responsibilities:
//!   1. Build a DepositNote carrying the bridged USDC amount (or its
//!      basket-constituent decomposition).
//!   2. Submit the note from the Darwin relay wallet to the v4
//!      controller for the requested basket.
//!   3. Wait for the controller to consume it and report the resulting
//!      basket-token amount minted into the controller's private
//!      vault.
//!
//! Two impls:
//!
//! - [`MockMidenSubmitter`]: instant in-memory submitter that returns
//!   deterministic fake tx hashes + a pro-rata basket amount.
//!   Used in unit tests so the relay FSM can be exercised end-to-end
//!   without spinning up miden-client.
//! - `LiveMidenSubmitter` (gated behind `miden-live` feature, iter 4):
//!   real miden-client submission against rpc.testnet.miden.io.

mod mock;

pub use mock::MockMidenSubmitter;

use async_trait::async_trait;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MidenSubmitOutcome {
    /// Tx hash of the controller consuming the deposit note. Persisted
    /// in `DepositRecord.miden_consume_tx`.
    pub consume_tx: String,
    /// Net basket-token amount minted into the controller's private
    /// vault after the controller's fee skim. Persisted in
    /// `DepositRecord.basket_amount_minted` so the ETH-side mint can
    /// echo the same number to the user.
    pub basket_amount_minted: u128,
}

#[derive(Debug, thiserror::Error)]
pub enum MidenError {
    #[error("miden transient: {0}")]
    Transient(String),
    #[error("miden permanent: {0}")]
    Permanent(String),
}

#[async_trait]
pub trait MidenSubmitter: Send + Sync {
    /// Build + submit + wait-for-consume of a deposit note for
    /// `deposit_id`. Carries `amount_usdc` worth of asset (bridged
    /// from ETH side) into the basket whose on-chain commitment
    /// matches `basket_id`.
    async fn submit_deposit(
        &self,
        deposit_id: u64,
        basket_id: &str,
        amount_usdc: u128,
    ) -> Result<MidenSubmitOutcome, MidenError>;
}
