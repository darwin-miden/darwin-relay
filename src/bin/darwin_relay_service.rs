//! Darwin relay service entry point.
//!
//! Today this runs the full ingest+driver runtime against mocks
//! (MockWatcher receives a few synthetic deposits, MockBridge +
//! MockEthClient + MockMidenSubmitter handle the rest). Watch it
//! drive each deposit to `Settled`, then exit when the watcher
//! closes and the driver quiets.
//!
//! Iteration 4 swaps the mock surfaces for `AlloyWatcher` +
//! `AlloyEthClient` + a Miden-live submitter, configurable via env
//! vars (DARWIN_RPC_URL, DARWIN_RELAY_ADDR, etc.).

use std::sync::Arc;
use std::time::Duration;

use darwin_relay::bridge::MockBridge;
use darwin_relay::eth::MockEthClient;
use darwin_relay::miden::MockMidenSubmitter;
use darwin_relay::runtime::{spawn_runtime, RelayConfig};
use darwin_relay::service::RelayService;
use darwin_relay::store::DepositStore;
use darwin_relay::watcher::{MockWatcher, ObservedDeposit};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let store = Arc::new(DepositStore::open_in_memory()?);
    let bridge = Arc::new(MockBridge::new());
    let eth = Arc::new(MockEthClient::new());
    let miden = Arc::new(MockMidenSubmitter::new());
    let svc = Arc::new(RelayService::new(
        store.clone(),
        bridge.clone(),
        eth.clone(),
        miden.clone(),
    ));

    let (watcher, watcher_handle) = MockWatcher::new(16);

    let cfg = RelayConfig {
        driver_tick: Duration::from_millis(50),
        run_until_quiet: true,
        quiet_after: Duration::from_millis(500),
    };
    let rt = spawn_runtime(cfg, store.clone(), watcher, svc);

    // Push 3 sample deposits, then close the watcher.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    for id in 1..=3 {
        watcher_handle
            .push(ObservedDeposit {
                id,
                user_eth: format!("0xUser{id:040}"),
                basket_id: "0xdcc_basket_id_placeholder".into(),
                miden_recipient: "0x0".into(),
                amount_usdc: 1_000_000_000 * id as u128, // 1k, 2k, 3k USDC
                requested_at_unix: now,
            })
            .await;
    }
    watcher_handle.close();

    rt.join().await?;

    println!("\nFinal deposit ledger:");
    for id in 1..=3 {
        let r = store.get(id)?.unwrap();
        println!(
            "  id={} status={:<10} basket_amount={:?} claim={:?} mint={:?} confirm={:?}",
            r.id,
            r.status.as_str(),
            r.basket_amount_minted,
            r.claim_tx.as_deref().map(|s| &s[..14]),
            r.erc20_mint_tx.as_deref().map(|s| &s[..14]),
            r.confirm_tx.as_deref().map(|s| &s[..14]),
        );
    }

    println!("\nMockEthClient call trace:");
    for (i, call) in eth.calls().iter().enumerate() {
        println!("  [{i}] {call:?}");
    }
    println!("\nMockMidenSubmitter call trace:");
    for (i, call) in miden.calls().iter().enumerate() {
        println!("  [{i}] {call:?}");
    }
    Ok(())
}
