//! Darwin Relay service entry point.
//!
//! Two modes:
//!
//! - **mock** (default): MockWatcher + MockBridge + MockEthClient +
//!   MockMidenSubmitter. Useful as a smoke run that always exits
//!   cleanly. `cargo run --bin darwin_relay_service`.
//!
//! - **live** (`--mode live`, requires `--features miden-live`):
//!   AlloyWatcher subscribed to the deployed `DarwinRelayDeposit.sol`
//!   on Sepolia + AlloyEthClient for ETH writes + LiveMidenSubmitter
//!   talking to rpc.testnet.miden.io. MockBridge still used for the
//!   AggLayer leg until the canonical bridge is publicly live.
//!
//! Env vars (live mode):
//!
//! - `DARWIN_RELAY_ETH_RPC_WS`        ws://… or wss://… for the
//!                                     subscriber
//! - `DARWIN_RELAY_ETH_RPC_HTTP`      http(s)://… for write txs
//! - `DARWIN_RELAY_ETH_CONTRACT`      0x… DarwinRelayDeposit address
//! - `DARWIN_RELAY_ETH_OPERATOR_KEY`  0x… 32-byte private key for
//!                                     the operator EOA (claim,
//!                                     confirm, refund, mintTo)
//! - `DARWIN_RELAY_ETH_BASKETS`       JSON map basketIdHex →
//!                                     basketTokenAddress
//! - `DARWIN_RELAY_MIDEN_WALLET`      0x… Miden relay-wallet account
//!                                     id (created via
//!                                     `miden client new-wallet`)
//! - `DARWIN_RELAY_MIDEN_CONTROLLER`  0x… v4 controller account id
//! - `DARWIN_RELAY_MIDEN_STABLE_FAUCET` 0x… stable faucet on Miden
//!                                     used as the bridged-USDC
//!                                     mirror (default: dUSDT
//!                                     faucet on testnet)
//! - `DARWIN_RELAY_STORE`             path to the relay's SQLite
//!                                     file (default in-memory)

use std::env;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Mock,
    Live,
}

fn parse_mode() -> Mode {
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--mode" {
            match args.next().as_deref() {
                Some("mock") => return Mode::Mock,
                Some("live") => return Mode::Live,
                other => {
                    eprintln!("unknown --mode {other:?} (use mock|live)");
                    std::process::exit(2);
                }
            }
        }
    }
    Mode::Mock
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    match parse_mode() {
        Mode::Mock => run_mock().await,
        Mode::Live => run_live().await,
    }
}

async fn run_mock() -> anyhow::Result<()> {
    tracing::info!("starting darwin-relay in MOCK mode");

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
                amount_usdc: 1_000_000_000 * id as u128,
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
            "  id={} status={} amount_usdc={} basket_amount={:?}",
            r.id,
            r.status.as_str(),
            r.amount_usdc,
            r.basket_amount_minted,
        );
    }
    Ok(())
}

#[cfg(not(feature = "miden-live"))]
async fn run_live() -> anyhow::Result<()> {
    anyhow::bail!(
        "live mode requires building with --features miden-live; rebuild with:\n  \
         cargo build --features miden-live --bin darwin_relay_service"
    )
}

#[cfg(feature = "miden-live")]
async fn run_live() -> anyhow::Result<()> {
    use darwin_relay::miden::{LiveMidenConfig, LiveMidenSubmitter};

    tracing::info!("starting darwin-relay in LIVE mode");

    // Miden config from env. Required.
    let miden_cfg = LiveMidenConfig::from_env().ok_or_else(|| {
        anyhow::anyhow!(
            "LIVE mode requires DARWIN_RELAY_MIDEN_WALLET, \
             DARWIN_RELAY_MIDEN_CONTROLLER, DARWIN_RELAY_MIDEN_STABLE_FAUCET"
        )
    })?;
    let miden = Arc::new(LiveMidenSubmitter::spawn(miden_cfg)?);

    // ETH side is intentionally still mocked in iter 4 — the AlloyWatcher
    // + AlloyEthClient need a deployed escrow address on Sepolia + a
    // funded operator key, which we don't commit yet. Set up:
    //
    //   1. Deploy DarwinRelayDeposit on Sepolia (script in
    //      contracts/script/DeployRelayStack.s.sol, iter 4 task #91).
    //   2. Export DARWIN_RELAY_ETH_RPC_WS / _CONTRACT / _OPERATOR_KEY.
    //   3. Swap the MockEthClient + MockWatcher below for
    //      `darwin_relay::eth::connect_http_alloy_eth_client` +
    //      `darwin_relay::watcher::AlloyWatcher::start`.
    let eth = Arc::new(MockEthClient::new());
    let bridge = Arc::new(MockBridge::new());
    let (watcher, watcher_handle) = MockWatcher::new(16);

    let store_path = std::env::var("DARWIN_RELAY_STORE")
        .map(std::path::PathBuf::from)
        .ok();
    let store = Arc::new(match store_path {
        Some(p) => {
            let _ = std::fs::remove_file(&p);
            DepositStore::open(p)?
        }
        None => DepositStore::open_in_memory()?,
    });

    let svc = Arc::new(RelayService::new(
        store.clone(),
        bridge,
        eth.clone(),
        miden.clone(),
    ));

    let cfg = RelayConfig {
        driver_tick: Duration::from_secs(2),
        run_until_quiet: true,
        quiet_after: Duration::from_secs(10),
    };
    let rt = spawn_runtime(cfg, store.clone(), watcher, svc);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    watcher_handle
        .push(ObservedDeposit {
            id: 1,
            user_eth: "0xLiveTestUser".into(),
            basket_id: "0xdcc_basket_id_placeholder".into(),
            miden_recipient: "0x0".into(),
            amount_usdc: 1_000_000, // 1 USDC
            requested_at_unix: now,
        })
        .await;
    watcher_handle.close();

    rt.join().await?;

    let r = store.get(1)?.unwrap();
    println!("\nLive deposit result:");
    println!("  id              {}", r.id);
    println!("  status          {}", r.status.as_str());
    println!("  miden_consume_tx {:?}", r.miden_consume_tx);
    println!("  basket_amount   {:?}", r.basket_amount_minted);

    Ok(())
}
