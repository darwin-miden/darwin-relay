//! Real `MidenSubmitter` over miden-client 0.14.
//!
//! miden-client is `!Send` (its sync stream holds `dyn Future`s
//! without Send bounds), so we can't call it directly from a
//! `tokio::spawn` future that requires `Send`. Workaround: spawn a
//! dedicated worker thread with its own single-thread tokio runtime,
//! and ferry submission requests over an mpsc channel. The
//! `LiveMidenSubmitter` exposed to the rest of the crate is just the
//! Send-safe handle.
//!
//! Gated behind the `miden-live` Cargo feature.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use miden_client::account::AccountId;
use miden_client::asset::{Asset, FungibleAsset};
use miden_client::builder::ClientBuilder;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::transaction::{PaymentNoteDescription, TransactionRequestBuilder};
use miden_client_sqlite_store::SqliteStore;
use tokio::sync::{mpsc, oneshot};

use super::{MidenError, MidenSubmitOutcome, MidenSubmitter};

#[derive(Debug, Clone)]
pub struct LiveMidenConfig {
    pub relay_wallet_hex: String,
    pub controller_hex: String,
    /// Faucet used as the "USDC-equivalent" on Miden. For M2 iter 4
    /// we route bridged USDC into the existing dUSDT faucet (closest
    /// 6-decimal stable proxy on testnet).
    pub stable_faucet_hex: String,
    pub store_path: PathBuf,
    pub keystore_path: PathBuf,
}

impl LiveMidenConfig {
    pub fn from_env() -> Option<Self> {
        let home = std::env::var("HOME").ok()?;
        Some(Self {
            relay_wallet_hex: std::env::var("DARWIN_RELAY_MIDEN_WALLET").ok()?,
            controller_hex: std::env::var("DARWIN_RELAY_MIDEN_CONTROLLER").ok()?,
            stable_faucet_hex: std::env::var("DARWIN_RELAY_MIDEN_STABLE_FAUCET").ok()?,
            store_path: std::env::var("DARWIN_RELAY_MIDEN_STORE")
                .map(PathBuf::from)
                .unwrap_or_else(|_| format!("{home}/.miden/darwin_relay.sqlite3").into()),
            keystore_path: std::env::var("DARWIN_RELAY_MIDEN_KEYSTORE")
                .map(PathBuf::from)
                .unwrap_or_else(|_| format!("{home}/.miden/keystore").into()),
        })
    }
}

struct SubmitJob {
    deposit_id: u64,
    amount_usdc: u128,
    reply: oneshot::Sender<Result<MidenSubmitOutcome, MidenError>>,
}

/// Send-safe handle to the dedicated miden-client worker thread.
/// Cloneable across runtime tasks.
pub struct LiveMidenSubmitter {
    tx: mpsc::Sender<SubmitJob>,
}

impl LiveMidenSubmitter {
    /// Spawn the worker thread, connect miden-client inside it, and
    /// return a handle the rest of the relay can call. Errors out of
    /// the worker thread (e.g. miden-client connect failure) bubble
    /// up the first time `submit_deposit` is called.
    pub fn spawn(cfg: LiveMidenConfig) -> Result<Self, MidenError> {
        let (tx, rx) = mpsc::channel::<SubmitJob>(32);
        std::thread::Builder::new()
            .name("darwin-relay-miden-worker".into())
            .spawn(move || worker_main(cfg, rx))
            .map_err(|e| MidenError::Permanent(format!("worker spawn: {e}")))?;
        Ok(Self { tx })
    }
}

#[async_trait]
impl MidenSubmitter for LiveMidenSubmitter {
    async fn submit_deposit(
        &self,
        deposit_id: u64,
        _basket_id: &str,
        amount_usdc: u128,
    ) -> Result<MidenSubmitOutcome, MidenError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(SubmitJob {
                deposit_id,
                amount_usdc,
                reply: reply_tx,
            })
            .await
            .map_err(|_| MidenError::Permanent("worker channel closed".into()))?;
        reply_rx
            .await
            .map_err(|_| MidenError::Permanent("worker dropped reply".into()))?
    }
}

fn worker_main(cfg: LiveMidenConfig, mut rx: mpsc::Receiver<SubmitJob>) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("worker rt");
    rt.block_on(async move {
        let mut state = match WorkerState::connect(cfg).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "miden worker failed to connect");
                // Drain incoming jobs so callers don't hang.
                while let Some(job) = rx.recv().await {
                    let _ = job.reply.send(Err(MidenError::Permanent(format!(
                        "worker init failed: {e}"
                    ))));
                }
                return;
            }
        };
        while let Some(job) = rx.recv().await {
            let out = state.run_one(job.deposit_id, job.amount_usdc).await;
            let _ = job.reply.send(out);
        }
        tracing::info!("miden worker channel closed; exiting");
    });
}

struct WorkerState {
    relay_wallet: AccountId,
    controller: AccountId,
    stable_faucet: AccountId,
    client: miden_client::Client<FilesystemKeyStore>,
}

impl WorkerState {
    async fn connect(cfg: LiveMidenConfig) -> Result<Self, MidenError> {
        let relay_wallet = AccountId::from_hex(&cfg.relay_wallet_hex)
            .map_err(|e| MidenError::Permanent(format!("relay_wallet_hex: {e}")))?;
        let controller = AccountId::from_hex(&cfg.controller_hex)
            .map_err(|e| MidenError::Permanent(format!("controller_hex: {e}")))?;
        let stable_faucet = AccountId::from_hex(&cfg.stable_faucet_hex)
            .map_err(|e| MidenError::Permanent(format!("stable_faucet_hex: {e}")))?;
        let _ = std::fs::remove_file(&cfg.store_path);
        let store = SqliteStore::new(cfg.store_path.clone())
            .await
            .map_err(|e| MidenError::Transient(format!("store init: {e}")))?;
        let mut client = ClientBuilder::<FilesystemKeyStore>::new()
            .grpc_client(&miden_client::rpc::Endpoint::testnet(), None)
            .store(Arc::new(store))
            .filesystem_keystore(cfg.keystore_path.clone())
            .map_err(|e| MidenError::Transient(format!("keystore: {e}")))?
            .build()
            .await
            .map_err(|e| MidenError::Transient(format!("client build: {e}")))?;
        client
            .sync_state()
            .await
            .map_err(|e| MidenError::Transient(format!("sync_state: {e}")))?;
        let _ = client.import_account_by_id(controller).await;
        let _ = client.import_account_by_id(stable_faucet).await;
        Ok(Self {
            relay_wallet,
            controller,
            stable_faucet,
            client,
        })
    }

    async fn run_one(
        &mut self,
        deposit_id: u64,
        amount_usdc: u128,
    ) -> Result<MidenSubmitOutcome, MidenError> {
        let amount: u64 = amount_usdc
            .try_into()
            .map_err(|_| MidenError::Permanent("amount overflows u64".into()))?;
        let asset = FungibleAsset::new(self.stable_faucet, amount)
            .map_err(|e| MidenError::Permanent(format!("FungibleAsset: {e}")))?;

        self.client
            .sync_state()
            .await
            .map_err(|e| MidenError::Transient(format!("sync_state: {e}")))?;

        let description = PaymentNoteDescription::new(
            vec![Asset::Fungible(asset)],
            self.relay_wallet,
            self.controller,
        );
        let rng = self.client.rng();
        let request = TransactionRequestBuilder::new()
            .build_pay_to_id(description, miden_client::note::NoteType::Private, rng)
            .map_err(|e| MidenError::Transient(format!("build P2ID: {e}")))?;

        let result = self
            .client
            .execute_transaction(self.relay_wallet, request)
            .await
            .map_err(|e| MidenError::Transient(format!("execute_transaction: {e}")))?;
        let consume_tx_id = result.executed_transaction().id();
        let prover = self.client.prover();
        let proven = self
            .client
            .prove_transaction_with(&result, prover)
            .await
            .map_err(|e| MidenError::Transient(format!("prove_transaction: {e}")))?;
        let height = self
            .client
            .submit_proven_transaction(proven, &result)
            .await
            .map_err(|e| MidenError::Transient(format!("submit_proven: {e}")))?;
        self.client
            .apply_transaction(&result, height)
            .await
            .map_err(|e| MidenError::Transient(format!("apply_transaction: {e}")))?;

        tracing::info!(
            deposit_id,
            tx = %consume_tx_id,
            block = %height,
            amount,
            "live miden submit confirmed"
        );

        Ok(MidenSubmitOutcome {
            consume_tx: format!("{consume_tx_id}"),
            basket_amount_minted: amount_usdc,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_env_returns_none_when_unset() {
        std::env::remove_var("DARWIN_RELAY_MIDEN_WALLET");
        assert!(LiveMidenConfig::from_env().is_none());
    }
}
