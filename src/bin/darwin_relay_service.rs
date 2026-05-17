//! Darwin Relay service entry point.
//!
//! - `--mode mock` (default): all-mock smoke run. 3 sample deposits
//!   pushed through MockWatcher, runtime drives them to Settled.
//!
//! - `--mode live`: bring up live wires when env is set.
//!     - DARWIN_RELAY_ETH_*       → AlloyWatcher + AlloyEthClient
//!     - DARWIN_RELAY_MIDEN_*     → LiveMidenSubmitter (requires
//!                                  --features miden-live at build)
//!
//!   You can mix and match: set only ETH vars to drive Sepolia events
//!   into a Mock Miden submitter; set only Miden vars for the reverse;
//!   set both for the full real-wire stack.
//!
//! ETH env vars:
//!   DARWIN_RELAY_ETH_RPC_HTTP      https:// JSON-RPC for write txs
//!   DARWIN_RELAY_ETH_RPC_WS        wss://   for log subscriptions
//!   DARWIN_RELAY_ETH_OPERATOR_KEY  0x… 32-byte private key (operator)
//!   DARWIN_RELAY_ETH_CONTRACT      0x… deployed DarwinRelayDeposit
//!   DARWIN_RELAY_ETH_BASKETS       optional JSON map basketIdHex → tokenAddr
//!
//! Miden env vars (only used with --features miden-live):
//!   DARWIN_RELAY_MIDEN_WALLET         relay wallet account id
//!   DARWIN_RELAY_MIDEN_CONTROLLER     v4 controller account id
//!   DARWIN_RELAY_MIDEN_STABLE_FAUCET  bridged-USDC mirror faucet id

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
    let bridge: Arc<dyn darwin_relay::bridge::BridgeClient> = Arc::new(MockBridge::new());
    let eth: Arc<dyn darwin_relay::eth::EthClient> = Arc::new(MockEthClient::new());
    let miden: Arc<dyn darwin_relay::miden::MidenSubmitter> =
        Arc::new(MockMidenSubmitter::new());
    let svc = Arc::new(RelayService::new(store.clone(), bridge, eth, miden));
    let (watcher, watcher_handle) = MockWatcher::new(16);

    let cfg = RelayConfig {
        driver_tick: Duration::from_millis(50),
        run_until_quiet: true,
        quiet_after: Duration::from_millis(500),
    };
    let rt = spawn_runtime(cfg, store.clone(), Box::new(watcher), svc);

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

async fn run_live() -> anyhow::Result<()> {
    use darwin_relay::eth::{connect_http_alloy_eth_client, LiveEthConfig, MockEthClient};
    use darwin_relay::watcher::AlloyWatcher;
    use std::path::PathBuf;

    tracing::info!("starting darwin-relay in LIVE mode");

    // Open the persistent store. Path is configurable so a relay can
    // be restarted and resume mid-flight.
    let store_path = std::env::var("DARWIN_RELAY_STORE").ok().map(PathBuf::from);
    let store = Arc::new(match store_path {
        Some(p) => {
            tracing::info!(path = ?p, "opening persistent SQLite store");
            DepositStore::open(p)?
        }
        None => {
            tracing::info!("no DARWIN_RELAY_STORE set, using in-memory store");
            DepositStore::open_in_memory()?
        }
    });

    // ETH side: real watcher + client when env vars present, mocks
    // otherwise. We bring up both up front so a partial config (e.g.
    // RPC set but no operator key) fails loudly at boot.
    let eth_cfg = LiveEthConfig::from_env();
    let bridge = Arc::new(MockBridge::new());

    let (eth_box, watcher_box, eth_label): (
        Arc<dyn darwin_relay::eth::EthClient>,
        Box<dyn darwin_relay::watcher::DepositWatcher + Send>,
        &'static str,
    ) = match eth_cfg {
        Some(cfg) => {
            tracing::info!(
                rpc_http = %cfg.rpc_http,
                rpc_ws = %cfg.rpc_ws,
                relay = %cfg.relay_address,
                baskets = cfg.baskets.len(),
                "wiring AlloyEthClient + AlloyWatcher"
            );
            let alloy_eth = connect_http_alloy_eth_client(
                &cfg.rpc_http,
                &cfg.operator_key_hex,
                cfg.relay_address,
                cfg.baskets,
            )
            .await
            .map_err(|e| anyhow::anyhow!("alloy eth client: {e}"))?;
            let alloy_watcher =
                AlloyWatcher::start(&cfg.rpc_ws, cfg.relay_address)
                    .await
                    .map_err(|e| anyhow::anyhow!("alloy watcher: {e}"))?;
            (
                Arc::new(alloy_eth) as Arc<dyn darwin_relay::eth::EthClient>,
                Box::new(alloy_watcher),
                "alloy",
            )
        }
        None => {
            tracing::warn!(
                "DARWIN_RELAY_ETH_* not set; falling back to MockEthClient + MockWatcher"
            );
            let (w, _h) = MockWatcher::new(16);
            (
                Arc::new(MockEthClient::new()) as Arc<dyn darwin_relay::eth::EthClient>,
                Box::new(w),
                "mock",
            )
        }
    };

    // Miden side: same pattern.
    let miden_box: Arc<dyn darwin_relay::miden::MidenSubmitter> = build_miden_submitter()?;

    tracing::info!(eth = eth_label, "submitting to runtime");

    let svc = Arc::new(RelayService::new(
        store.clone(),
        bridge,
        eth_box,
        miden_box,
    ));

    // In live mode we don't auto-exit. Caller stops with Ctrl-C.
    let cfg = RelayConfig {
        driver_tick: Duration::from_secs(2),
        run_until_quiet: false,
        quiet_after: Duration::from_secs(0),
    };
    let rt = spawn_runtime(cfg, store, watcher_box, svc);
    rt.join().await
}

#[cfg(not(feature = "miden-live"))]
fn build_miden_submitter(
) -> anyhow::Result<Arc<dyn darwin_relay::miden::MidenSubmitter>> {
    tracing::info!(
        "miden-live feature disabled; using MockMidenSubmitter \
         (rebuild with --features miden-live for real submission)"
    );
    Ok(Arc::new(darwin_relay::miden::MockMidenSubmitter::new()))
}

#[cfg(feature = "miden-live")]
fn build_miden_submitter(
) -> anyhow::Result<Arc<dyn darwin_relay::miden::MidenSubmitter>> {
    use darwin_relay::miden::{LiveMidenConfig, LiveMidenSubmitter};
    match LiveMidenConfig::from_env() {
        Some(cfg) => {
            tracing::info!(
                wallet = %cfg.relay_wallet_hex,
                controller = %cfg.controller_hex,
                "wiring LiveMidenSubmitter"
            );
            Ok(Arc::new(LiveMidenSubmitter::spawn(cfg).map_err(|e| {
                anyhow::anyhow!("live miden submitter spawn: {e}")
            })?))
        }
        None => {
            tracing::warn!(
                "DARWIN_RELAY_MIDEN_* not set; falling back to MockMidenSubmitter"
            );
            Ok(Arc::new(darwin_relay::miden::MockMidenSubmitter::new()))
        }
    }
}
