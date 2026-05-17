//! Service runtime — composes the watcher + driver loops into a
//! single long-running tokio task.
//!
//! Two concurrent loops:
//!
//! 1. **Ingest loop**: pulls `ObservedDeposit`s from the watcher,
//!    inserts each into the store as `Requested`.
//! 2. **Driver loop**: every `tick`, walks `store.list_open()` and
//!    calls `RelayService::drive(id)` on each open deposit.
//!
//! `RelayConfig` is the single struct callers build to bring up the
//! runtime. The binary populates it from env / CLI; tests populate
//! it inline.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::service::RelayService;
use crate::state::DepositRecord;
use crate::store::DepositStore;
use crate::watcher::DepositWatcher;

#[derive(Debug, Clone)]
pub struct RelayConfig {
    /// How often the driver loop wakes up to walk open deposits.
    pub driver_tick: Duration,
    /// If true, the driver loop exits after every open deposit reaches
    /// a terminal state and no new event arrives within `quiet_after`.
    /// Useful for tests; prod sets `run_until_quiet = false`.
    pub run_until_quiet: bool,
    pub quiet_after: Duration,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            driver_tick: Duration::from_secs(5),
            run_until_quiet: false,
            quiet_after: Duration::from_secs(2),
        }
    }
}

pub struct RelayHandle {
    pub ingest: JoinHandle<Result<()>>,
    pub driver: JoinHandle<Result<()>>,
}

impl RelayHandle {
    /// Wait for both loops to finish. Returns whichever errored first.
    pub async fn join(self) -> Result<()> {
        let (ingest_res, driver_res) = tokio::join!(self.ingest, self.driver);
        ingest_res??;
        driver_res??;
        Ok(())
    }
}

/// Spawn the ingest + driver loops for a relay configured with the
/// given watcher / bridge / eth / miden impls. Returns a handle the
/// caller can await on.
pub fn spawn_runtime(
    config: RelayConfig,
    store: Arc<DepositStore>,
    mut watcher: Box<dyn DepositWatcher + Send>,
    svc: Arc<RelayService>,
) -> RelayHandle {
    let ingest_store = store.clone();
    let ingest_cfg = config.clone();
    let ingest = tokio::spawn(async move {
        loop {
            match watcher.next().await {
                Ok(obs) => {
                    let rec = DepositRecord::new(
                        obs.id,
                        obs.user_eth,
                        obs.basket_id,
                        obs.miden_recipient,
                        obs.amount_usdc,
                        obs.requested_at_unix,
                    );
                    if let Err(e) = ingest_store.insert(&rec) {
                        warn!(id = rec.id, error = %e, "ingest: insert failed (likely duplicate)");
                    } else {
                        info!(id = rec.id, "ingest: new deposit");
                    }
                }
                Err(crate::watcher::WatcherError::Closed) => {
                    info!("watcher closed; ingest loop ending");
                    if ingest_cfg.run_until_quiet {
                        return Ok::<(), anyhow::Error>(());
                    }
                    return Err(anyhow::anyhow!("watcher closed unexpectedly"));
                }
                Err(e) => {
                    error!(error = %e, "ingest: watcher error");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    let driver_svc = svc;
    let driver_store = store;
    let driver_cfg = config;
    let driver = tokio::spawn(async move {
        let mut last_active = std::time::Instant::now();
        loop {
            let open = driver_store.list_open()?;
            if open.is_empty() && driver_cfg.run_until_quiet {
                if last_active.elapsed() >= driver_cfg.quiet_after {
                    info!("driver: quiet → exiting");
                    return Ok::<(), anyhow::Error>(());
                }
            } else if !open.is_empty() {
                last_active = std::time::Instant::now();
                for r in open {
                    if let Err(e) = driver_svc.drive(r.id).await {
                        warn!(id = r.id, error = %e, "drive failed; retrying next tick");
                    }
                }
            }
            tokio::time::sleep(driver_cfg.driver_tick).await;
        }
    });

    RelayHandle { ingest, driver }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::MockBridge;
    use crate::eth::MockEthClient;
    use crate::miden::MockMidenSubmitter;
    use crate::state::DepositStatus;
    use crate::watcher::{MockWatcher, ObservedDeposit};

    fn obs(id: u64) -> ObservedDeposit {
        ObservedDeposit {
            id,
            user_eth: "0xBeefBeefBeefBeefBeefBeefBeefBeefBeefBeef".into(),
            basket_id: "0xdcc".into(),
            miden_recipient: "0x0".into(),
            amount_usdc: 1_000_000,
            requested_at_unix: 1_700_000_000,
        }
    }

    #[tokio::test]
    async fn runtime_ingests_then_drives_each_deposit_to_settled() {
        let store = Arc::new(DepositStore::open_in_memory().unwrap());
        let bridge = Arc::new(MockBridge::new());
        let eth = Arc::new(MockEthClient::new());
        let miden = Arc::new(MockMidenSubmitter::new());
        let svc = Arc::new(RelayService::new(
            store.clone(),
            bridge,
            eth,
            miden,
        ));

        let (watcher, handle) = MockWatcher::new(8);
        let cfg = RelayConfig {
            driver_tick: Duration::from_millis(20),
            run_until_quiet: true,
            quiet_after: Duration::from_millis(200),
        };
        let rt = spawn_runtime(cfg, store.clone(), Box::new(watcher), svc);

        // Push 3 deposits, then close the watcher so ingest can drain.
        for id in 1..=3 {
            handle.push(obs(id)).await;
        }
        handle.close();

        rt.join().await.unwrap();

        // All 3 must have reached terminal state
        for id in 1..=3 {
            let r = store.get(id).unwrap().unwrap();
            assert_eq!(r.status, DepositStatus::Settled, "deposit {id} settled");
        }
        assert!(store.list_open().unwrap().is_empty());
    }

    #[tokio::test]
    async fn runtime_handles_mixed_success_and_failure() {
        let store = Arc::new(DepositStore::open_in_memory().unwrap());
        let bridge = Arc::new(MockBridge::new());
        let eth = Arc::new(MockEthClient::new().fail_on_claim(2));
        let miden = Arc::new(MockMidenSubmitter::new());
        let svc = Arc::new(RelayService::new(
            store.clone(),
            bridge,
            eth,
            miden,
        ));

        let (watcher, handle) = MockWatcher::new(8);
        let cfg = RelayConfig {
            driver_tick: Duration::from_millis(20),
            run_until_quiet: true,
            quiet_after: Duration::from_millis(200),
        };
        let rt = spawn_runtime(cfg, store.clone(), Box::new(watcher), svc);

        handle.push(obs(1)).await;
        handle.push(obs(2)).await; // will fail on claim
        handle.push(obs(3)).await;
        handle.close();

        rt.join().await.unwrap();

        assert_eq!(store.get(1).unwrap().unwrap().status, DepositStatus::Settled);
        assert_eq!(store.get(2).unwrap().unwrap().status, DepositStatus::Refunded);
        assert_eq!(store.get(3).unwrap().unwrap().status, DepositStatus::Settled);
    }
}
