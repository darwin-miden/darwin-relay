//! Send a P2ID note at a NETWORK account and let the testnet's Network
//! Transaction Builder consume it.
//!
//! The note is a plain public P2ID carrying dUSDC, with a
//! `NetworkAccountTarget` attachment so the NTB routes it to the target
//! account's actor. If the target's `AuthNetworkAccount` allowlist
//! contains the P2ID script root, the NETWORK executes the consume —
//! no operator key, no relay tx.
//!
//! Usage:
//!     cargo run --release --features v2-worker --bin send_network_note -- \
//!         --target 0x1d74ec8a959cb2112dc06a66f73845 --amount 100000
//!
//! Sender defaults to the relay wallet (key in the relay keystore).

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use miden_client::account::AccountId;
use miden_client::asset::{Asset, FungibleAsset};
use miden_client::builder::ClientBuilder;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::NoteType;
use miden_client::transaction::TransactionRequestBuilder;
use miden_client_sqlite_store::SqliteStore;
use miden_protocol::note::{NoteAttachment, NoteAttachments};
use miden_standards::note::{NetworkAccountTarget, NoteExecutionHint, P2idNote};

const DEFAULT_SENDER: &str = "0x66e7105ea36a7491325480accb7331";
const DEFAULT_DUSDC: &str = "0xfc90f0f4da30e51168453b60eafed7";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut target: Option<String> = None;
    let mut sender = DEFAULT_SENDER.to_string();
    let mut faucet = DEFAULT_DUSDC.to_string();
    let mut amount: u64 = 100_000; // 0.1 dUSDC (6 dec)
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--target" => target = Some(args.next().context("--target value")?),
            "--sender" => sender = args.next().context("--sender value")?,
            "--faucet" => faucet = args.next().context("--faucet value")?,
            "--amount" => amount = args.next().context("--amount value")?.parse()?,
            _ => {}
        }
    }
    let target = AccountId::from_hex(&target.context("--target required")?)?;
    let sender = AccountId::from_hex(&sender)?;
    let faucet = AccountId::from_hex(&faucet)?;

    let home = std::env::var("HOME")?;
    let store_path = std::env::var("DARWIN_RELAY_V2_MIDEN_STORE").unwrap_or_else(|_| {
        format!("{home}/data/darwin/.relay-miden-testnet/.miden/store.sqlite3")
    });
    let keystore_path = std::env::var("DARWIN_RELAY_V2_MIDEN_KEYSTORE").unwrap_or_else(|_| {
        format!("{home}/data/darwin/.relay-miden-testnet/.miden/keystore")
    });

    println!("Connecting miden-client (testnet)…");
    let store = SqliteStore::new(PathBuf::from(&store_path)).await?;
    let endpoint = miden_client::rpc::Endpoint::testnet();
    let mut client = ClientBuilder::<FilesystemKeyStore>::new()
        .grpc_client(&endpoint, None)
        .store(Arc::new(store))
        .filesystem_keystore(PathBuf::from(&keystore_path))?
        .build()
        .await?;
    client.sync_state().await?;

    let assets = vec![Asset::Fungible(FungibleAsset::new(faucet, amount)?)];

    // The NetworkAccountTarget attachment tells the NTB which network
    // account this note is meant for; Always = consumable immediately.
    let na_target = NetworkAccountTarget::new(target, NoteExecutionHint::Always)
        .map_err(|e| anyhow::anyhow!("NetworkAccountTarget: {e:?}"))?;
    let attachments = NoteAttachments::new(vec![NoteAttachment::from(na_target)])
        .map_err(|e| anyhow::anyhow!("NoteAttachments: {e:?}"))?;

    let note = P2idNote::create(
        sender,
        target,
        assets,
        NoteType::Public,
        attachments,
        client.rng(),
    )
    .map_err(|e| anyhow::anyhow!("P2idNote::create: {e:?}"))?;
    let note_id = note.id();

    println!("Emitting network-targeted P2ID note…");
    println!("    sender : {}", sender.to_hex());
    println!("    target : {} (network account)", target.to_hex());
    println!("    asset  : {amount} base units of {}", faucet.to_hex());
    println!("    note id: {note_id}");

    let req = TransactionRequestBuilder::new()
        .own_output_notes(vec![note])
        .build()?;
    let result = client.execute_transaction(sender, req).await?;
    let tx_id = result.executed_transaction().id();
    let prover = client.prover();
    let proven = client.prove_transaction_with(&result, prover.clone()).await?;
    let height = client.submit_proven_transaction(proven, &result).await?;
    client.apply_transaction(&result, height).await?;

    println!();
    println!("✓ note emitted");
    println!("    emit tx : {tx_id}");
    println!("    height  : {height}");
    println!();
    println!("The NTB should now pick it up. Watch:");
    println!("    miden-client network-note-status {note_id}");

    Ok(())
}
