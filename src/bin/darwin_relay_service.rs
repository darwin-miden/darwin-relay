//! Stub entry point. The current scaffold doesn't watch a live RPC
//! yet; it prints the deposit FSM + bridge mock state so we have a
//! smoke target on `cargo run --bin darwin_relay_service`.
//!
//! Iteration 2 wires up the alloy WS subscriber on the
//! `RelayDepositRequested` event and starts the driver loop in
//! parallel.

use std::sync::Arc;

use darwin_relay::bridge::MockBridge;
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
    let svc = RelayService::new(store.clone(), bridge.clone());

    // Simulate an inbound RelayDepositRequested event for the smoke run.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let sample = DepositRecord::new(
        1,
        "0xBeefBeefBeefBeefBeefBeefBeefBeefBeefBeef".into(),
        "0xDCC_basket_id_keccak256_placeholder".into(),
        "0x0".into(), // no Miden recipient — wrapped ERC20 path
        1_000_000_000, // 1000 USDC at 6 decimals
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

    println!("\nMock bridge snapshot:");
    for (bref, id, amount, status) in bridge.snapshot() {
        println!("  {bref}  deposit={id}  amount={amount}  status={status:?}");
    }
    Ok(())
}
