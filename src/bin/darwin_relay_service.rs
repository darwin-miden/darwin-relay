//! Stub entry point. The current scaffold doesn't watch a live RPC
//! yet; it drives a single in-memory sample deposit through the full
//! FSM using MockBridge + MockEthClient, so `cargo run` always
//! exercises every transition.
//!
//! Iteration 3 swaps the mocks for AlloyEthClient + AggLayerBridge +
//! the real Miden submitter, and starts the alloy WS event watcher
//! in parallel.

use std::sync::Arc;

use darwin_relay::bridge::MockBridge;
use darwin_relay::eth::MockEthClient;
use darwin_relay::service::RelayService;
use darwin_relay::state::DepositRecord;
use darwin_relay::store::DepositStore;
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
    let svc = RelayService::new(store.clone(), bridge.clone(), eth.clone());

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let sample = DepositRecord::new(
        1,
        "0xBeefBeefBeefBeefBeefBeefBeefBeefBeefBeef".into(),
        "0xDCC_basket_id_keccak256_placeholder".into(),
        "0x0".into(),
        1_000_000_000,
        now,
    );
    store.insert(&sample)?;
    tracing::info!("inserted sample deposit, driving it through the FSM…");

    let final_status = svc.drive(1).await?;
    tracing::info!(?final_status, "deposit reached terminal state");

    let final_record = store.get(1)?.unwrap();
    println!("\nFinal deposit state:");
    println!("  id              {}", final_record.id);
    println!("  status          {}", final_record.status.as_str());
    println!("  amount_usdc     {}", final_record.amount_usdc);
    println!("  claim_tx        {:?}", final_record.claim_tx);
    println!("  bridge_tx       {:?}", final_record.bridge_tx);
    println!("  miden_consume_tx {:?}", final_record.miden_consume_tx);
    println!("  erc20_mint_tx   {:?}", final_record.erc20_mint_tx);
    println!("  confirm_tx      {:?}", final_record.confirm_tx);

    println!("\nMockEthClient call trace:");
    for (i, call) in eth.calls().iter().enumerate() {
        println!("  [{i}] {call:?}");
    }
    Ok(())
}
