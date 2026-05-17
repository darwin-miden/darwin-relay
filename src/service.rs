//! Relay service orchestrator (tokio-driven).
//!
//! Drives each open deposit through its FSM one step at a time:
//!
//! - `Requested → Claimed`: `EthClient::claim_deposit`
//! - `Claimed → BridgeInFlight | BridgedToMiden`: `BridgeClient::bridge_out`
//! - `BridgeInFlight → BridgedToMiden | Refunded`: `BridgeClient::poll`
//! - `BridgedToMiden → MidenMinted`: (iteration 3 — Miden submitter)
//! - `MidenMinted → Erc20Minted`: `EthClient::mint_basket_to`
//! - `Erc20Minted → Settled`: `EthClient::confirm_deposit`
//!
//! Permanent errors (e.g. unknown basket, bridge `Permanent`) route the
//! deposit to `Refunded` via `EthClient::refund_deposit`. Transient
//! errors bubble up so the outer loop can retry.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tracing::{info, warn};

use crate::bridge::{BridgeClient, BridgeError, BridgeStatus};
use crate::eth::{EthClient, EthError};
use crate::miden::{MidenError, MidenSubmitter};
use crate::state::{DepositRecord, DepositStatus};
use crate::store::{DepositStore, TxColumn};

pub struct RelayService<B: BridgeClient, E: EthClient, M: MidenSubmitter> {
    pub store: Arc<DepositStore>,
    pub bridge: Arc<B>,
    pub eth: Arc<E>,
    pub miden: Arc<M>,
    pub tick: Duration,
}

impl<B: BridgeClient + 'static, E: EthClient + 'static, M: MidenSubmitter + 'static>
    RelayService<B, E, M>
{
    pub fn new(
        store: Arc<DepositStore>,
        bridge: Arc<B>,
        eth: Arc<E>,
        miden: Arc<M>,
    ) -> Self {
        Self {
            store,
            bridge,
            eth,
            miden,
            tick: Duration::from_secs(5),
        }
    }

    /// Drive a single deposit one step forward in its FSM. Returns the
    /// new status. Idempotent on terminal states.
    pub async fn step(&self, r: &DepositRecord) -> Result<DepositStatus> {
        let now = unix_now();
        match r.status {
            DepositStatus::Requested => match self.eth.claim_deposit(r.id).await {
                Ok(tx) => {
                    self.store.set_tx(r.id, TxColumn::Claim, &tx, now)?;
                    self.store
                        .update_status(r.id, DepositStatus::Claimed, now)?;
                    info!(id = r.id, tx = %tx, "claimed");
                    Ok(DepositStatus::Claimed)
                }
                Err(EthError::Permanent(e)) => self.route_to_refund(r, &e).await,
                Err(EthError::Transient(e)) => Err(anyhow::anyhow!("claim transient: {e}")),
            },
            DepositStatus::Claimed => match self.bridge.bridge_out(r.id, r.amount_usdc).await {
                Ok(receipt) => {
                    self.store
                        .set_tx(r.id, TxColumn::Bridge, &receipt.bridge_ref, now)?;
                    let next = match receipt.status {
                        BridgeStatus::Confirmed => DepositStatus::BridgedToMiden,
                        BridgeStatus::Pending => DepositStatus::BridgeInFlight,
                        BridgeStatus::Failed => {
                            return self.route_to_refund(r, "bridge initial Failed").await;
                        }
                    };
                    self.store.update_status(r.id, next, now)?;
                    info!(id = r.id, ?next, "bridge initiated");
                    Ok(next)
                }
                Err(BridgeError::Permanent(e)) => self.route_to_refund(r, &e).await,
                Err(BridgeError::Transient(e)) => Err(anyhow::anyhow!("bridge transient: {e}")),
            },
            DepositStatus::BridgeInFlight => {
                let bridge_ref = r
                    .bridge_tx
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("BridgeInFlight without bridge_tx"))?;
                match self.bridge.poll(bridge_ref).await {
                    Ok(receipt) => match receipt.status {
                        BridgeStatus::Confirmed => {
                            self.store
                                .update_status(r.id, DepositStatus::BridgedToMiden, now)?;
                            info!(id = r.id, "bridge confirmed");
                            Ok(DepositStatus::BridgedToMiden)
                        }
                        BridgeStatus::Failed => {
                            self.route_to_refund(r, "bridge poll Failed").await
                        }
                        BridgeStatus::Pending => Ok(DepositStatus::BridgeInFlight),
                    },
                    Err(BridgeError::Permanent(e)) => self.route_to_refund(r, &e).await,
                    Err(BridgeError::Transient(e)) => {
                        Err(anyhow::anyhow!("bridge poll transient: {e}"))
                    }
                }
            }
            DepositStatus::BridgedToMiden => match self
                .miden
                .submit_deposit(r.id, &r.basket_id, r.amount_usdc)
                .await
            {
                Ok(outcome) => {
                    self.store
                        .set_tx(r.id, TxColumn::MidenConsume, &outcome.consume_tx, now)?;
                    self.store.set_basket_amount_minted(
                        r.id,
                        outcome.basket_amount_minted,
                        now,
                    )?;
                    self.store
                        .update_status(r.id, DepositStatus::MidenMinted, now)?;
                    info!(
                        id = r.id,
                        tx = %outcome.consume_tx,
                        basket_amount = outcome.basket_amount_minted,
                        "miden mint"
                    );
                    Ok(DepositStatus::MidenMinted)
                }
                Err(MidenError::Permanent(e)) => self.route_to_refund(r, &e).await,
                Err(MidenError::Transient(e)) => {
                    Err(anyhow::anyhow!("miden submit transient: {e}"))
                }
            },
            DepositStatus::MidenMinted => {
                // basket_amount_minted comes from the Miden submitter
                // in iter 3; for now we mint amount_usdc 1:1 minus the
                // 30 bps mint fee.
                let basket_amount = r
                    .basket_amount_minted
                    .unwrap_or_else(|| r.amount_usdc * 9970 / 10_000);
                match self
                    .eth
                    .mint_basket_to(&r.basket_id, &r.user_eth, basket_amount)
                    .await
                {
                    Ok(tx) => {
                        self.store.set_tx(r.id, TxColumn::Erc20Mint, &tx, now)?;
                        self.store
                            .update_status(r.id, DepositStatus::Erc20Minted, now)?;
                        info!(id = r.id, tx = %tx, basket_amount, "erc20 minted");
                        Ok(DepositStatus::Erc20Minted)
                    }
                    Err(EthError::Permanent(e)) => self.route_to_refund(r, &e).await,
                    Err(EthError::Transient(e)) => {
                        Err(anyhow::anyhow!("mint_basket_to transient: {e}"))
                    }
                }
            }
            DepositStatus::Erc20Minted => {
                let basket_amount = r
                    .basket_amount_minted
                    .unwrap_or_else(|| r.amount_usdc * 9970 / 10_000);
                match self.eth.confirm_deposit(r.id, basket_amount).await {
                    Ok(tx) => {
                        self.store.set_tx(r.id, TxColumn::Confirm, &tx, now)?;
                        self.store
                            .update_status(r.id, DepositStatus::Settled, now)?;
                        info!(id = r.id, tx = %tx, "settled");
                        Ok(DepositStatus::Settled)
                    }
                    Err(EthError::Permanent(e)) => self.route_to_refund(r, &e).await,
                    Err(EthError::Transient(e)) => {
                        Err(anyhow::anyhow!("confirm_deposit transient: {e}"))
                    }
                }
            }
            s if s.is_terminal() => Ok(s),
            s => Ok(s),
        }
    }

    /// Run a single deposit through every non-terminal step until
    /// terminal or until a step says "wait" (returns the same status).
    pub async fn drive(&self, id: u64) -> Result<DepositStatus> {
        loop {
            let r = self
                .store
                .get(id)?
                .ok_or_else(|| anyhow::anyhow!("unknown deposit {id}"))?;
            let prev = r.status;
            let next = self.step(&r).await?;
            if next == prev || next.is_terminal() {
                return Ok(next);
            }
        }
    }

    async fn route_to_refund(
        &self,
        r: &DepositRecord,
        reason: &str,
    ) -> Result<DepositStatus> {
        warn!(id = r.id, reason, "routing to refund");
        let now = unix_now();
        match self.eth.refund_deposit(r.id, reason).await {
            Ok(tx) => {
                self.store.set_tx(r.id, TxColumn::Refund, &tx, now)?;
                self.store
                    .update_status(r.id, DepositStatus::Refunded, now)?;
                Ok(DepositStatus::Refunded)
            }
            Err(e) => Err(anyhow::anyhow!("refund failed: {e}")),
        }
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::MockBridge;
    use crate::eth::{MockCall, MockEthClient};
    use crate::miden::MockMidenSubmitter;
    use crate::state::DepositRecord;

    fn sample(id: u64) -> DepositRecord {
        DepositRecord::new(
            id,
            "0xuser".into(),
            "0xbasket".into(),
            "0xrecipient".into(),
            1_000_000,
            unix_now(),
        )
    }

    fn svc_with_mocks() -> (
        Arc<DepositStore>,
        Arc<MockBridge>,
        Arc<MockEthClient>,
        Arc<MockMidenSubmitter>,
        RelayService<MockBridge, MockEthClient, MockMidenSubmitter>,
    ) {
        let store = Arc::new(DepositStore::open_in_memory().unwrap());
        let bridge = Arc::new(MockBridge::new());
        let eth = Arc::new(MockEthClient::new());
        let miden = Arc::new(MockMidenSubmitter::new());
        let svc = RelayService::new(store.clone(), bridge.clone(), eth.clone(), miden.clone());
        (store, bridge, eth, miden, svc)
    }

    #[tokio::test]
    async fn happy_path_drives_to_settled_and_records_each_call() {
        let (store, _, eth, miden, svc) = svc_with_mocks();
        store.insert(&sample(1)).unwrap();

        let final_status = svc.drive(1).await.unwrap();
        assert_eq!(final_status, DepositStatus::Settled);

        let r = store.get(1).unwrap().unwrap();
        assert!(r.claim_tx.is_some());
        assert!(r.bridge_tx.is_some());
        assert!(r.miden_consume_tx.is_some());
        assert!(r.erc20_mint_tx.is_some());
        assert!(r.confirm_tx.is_some());
        // Miden submitter applied 30 bps fee on 1_000_000 → 997_000
        assert_eq!(r.basket_amount_minted, Some(997_000));

        let calls = eth.calls();
        assert_eq!(calls.len(), 3);
        assert!(matches!(calls[0], MockCall::Claim(1)));
        assert!(matches!(calls[1], MockCall::MintTo { amount: 997_000, .. }));
        assert!(matches!(calls[2], MockCall::Confirm { id: 1, basket_amount: 997_000 }));

        assert_eq!(miden.calls().len(), 1);
        assert_eq!(miden.calls()[0].amount_usdc, 1_000_000);
    }

    #[tokio::test]
    async fn permanent_eth_failure_routes_to_refund() {
        let store = Arc::new(DepositStore::open_in_memory().unwrap());
        let bridge = Arc::new(MockBridge::new());
        let eth = Arc::new(MockEthClient::new().fail_on_claim(7));
        let miden = Arc::new(MockMidenSubmitter::new());
        let svc = RelayService::new(store.clone(), bridge, eth.clone(), miden);
        store.insert(&sample(7)).unwrap();

        let final_status = svc.drive(7).await.unwrap();
        assert_eq!(final_status, DepositStatus::Refunded);

        let r = store.get(7).unwrap().unwrap();
        assert!(r.refund_tx.is_some());
        assert!(matches!(eth.calls().last().unwrap(), MockCall::Refund { id: 7, .. }));
    }

    #[tokio::test]
    async fn permanent_bridge_failure_routes_to_refund() {
        let store = Arc::new(DepositStore::open_in_memory().unwrap());
        let bridge = Arc::new(MockBridge::new().fail_on_amount(1_000_000));
        let eth = Arc::new(MockEthClient::new());
        let miden = Arc::new(MockMidenSubmitter::new());
        let svc = RelayService::new(store.clone(), bridge, eth, miden);
        store.insert(&sample(3)).unwrap();

        assert_eq!(svc.drive(3).await.unwrap(), DepositStatus::Refunded);
        assert!(store.get(3).unwrap().unwrap().refund_tx.is_some());
    }

    #[tokio::test]
    async fn permanent_miden_failure_routes_to_refund() {
        let store = Arc::new(DepositStore::open_in_memory().unwrap());
        let bridge = Arc::new(MockBridge::new());
        let eth = Arc::new(MockEthClient::new());
        let miden = Arc::new(MockMidenSubmitter::new().fail_on_basket("0xbasket"));
        let svc = RelayService::new(store.clone(), bridge, eth.clone(), miden);
        store.insert(&sample(4)).unwrap();

        assert_eq!(svc.drive(4).await.unwrap(), DepositStatus::Refunded);
        let r = store.get(4).unwrap().unwrap();
        assert!(r.refund_tx.is_some());
        // basket_amount_minted should stay None when miden submit fails
        assert_eq!(r.basket_amount_minted, None);
    }

    #[tokio::test]
    async fn delayed_bridge_walks_through_pending_then_settles() {
        let store = Arc::new(DepositStore::open_in_memory().unwrap());
        let bridge = Arc::new(MockBridge::with_delay(60));
        let eth = Arc::new(MockEthClient::new());
        let miden = Arc::new(MockMidenSubmitter::new());
        let svc = RelayService::new(store.clone(), bridge, eth, miden);
        store.insert(&sample(2)).unwrap();

        assert_eq!(svc.drive(2).await.unwrap(), DepositStatus::Settled);
        assert!(store.get(2).unwrap().unwrap().bridge_tx.is_some());
    }

    #[tokio::test]
    async fn resume_picks_up_open_deposits_after_crash() {
        let (store, _, _, _, svc) = svc_with_mocks();

        store.insert(&sample(1)).unwrap();
        store.insert(&sample(2)).unwrap();
        store
            .update_status(1, DepositStatus::Claimed, unix_now())
            .unwrap();

        let open = store.list_open().unwrap();
        assert_eq!(open.len(), 2);
        for r in open {
            svc.drive(r.id).await.unwrap();
        }
        assert!(store.list_open().unwrap().is_empty());
    }
}
