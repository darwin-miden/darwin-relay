//! darwin-relay-v2 on-chain worker.
//!
//! The axum service in `darwin_relay_v2` keeps off-chain accounting in
//! sync with the 1Click bridge. The on-chain leg — submitting an
//! `atomic_deposit_note` from the relay's Miden wallet to the basket
//! controller — is handled here in a separate process because the
//! miden-client futures are `!Send` and would block axum's runtime.
//!
//! Loop:
//!   1. Sync the relay's miden-client local store.
//!   2. Drain any inbound P2ID notes targeted at the relay wallet
//!      (these are the 1Click bridge deliveries).
//!   3. SELECT intents WHERE stage='POSITION_CREDITED'
//!                       AND atomic_deposit_tx IS NULL.
//!   4. For each, check the relay wallet vault has enough of the dETH
//!      faucet to cover `amount_in_wei` (clamped to u64). If yes,
//!      submit an `atomic_deposit_note` tx that emits the note to the
//!      basket controller. Persist the resulting tx hash on the intent.
//!
//! This binary is gated under the `v2-worker` cargo feature.
//!
//!   cargo run --release --features v2-worker --bin darwin_relay_v2_worker

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use tracing::{error, info, warn};

use miden_assembly::ast::{Module, ModuleKind};
use miden_assembly::{Assembler, DefaultSourceManager, Path as MidenPath};
use miden_client::account::AccountId;
use miden_client::asset::{Asset, FungibleAsset};
use miden_client::builder::ClientBuilder;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::{
    Note, NoteAssets, NoteMetadata, NoteRecipient, NoteScript, NoteStorage, NoteType,
};
use miden_client::transaction::TransactionRequestBuilder;
use miden_client_sqlite_store::SqliteStore;
use miden_core_lib::CoreLibrary;
use miden_protocol::transaction::TransactionKernel;
use rand::RngCore;

// MASM sources vendored from darwin-protocol/crates/darwin-notes and
// darwin-protocol-account. Kept in sync manually; both repos pin the
// same miden-assembly 0.22 / miden-protocol 0.14 ABI.
const ATOMIC_DEPOSIT_NOTE_MASM: &str = include_str!("../../asm/atomic_deposit_note.masm");
const MATH_MASM: &str = include_str!("../../asm/lib/math.masm");

const DEFAULT_RELAY_WALLET_HEX: &str = "0xed3cd5befa3207805f8529207cfc0d";
const DEFAULT_CONTROLLER_HEX: &str = "0xa25aa0b00007688024b74b05a52aab";
// Miden testnet dETH faucet (the M1 deth-equivalent fungible faucet
// the basket controllers know about). The 1Click bridge mints
// `miden-testnet:eth` from a different faucet; for the deposit path
// to work, the relay wallet must hold dETH from a faucet the basket
// controller's `receive_asset` recognises. In M4 we'll route
// 1Click's eth-faucet output through a swap, but for the M3 demo
// the worker reads whichever fungible asset the relay holds — same
// dynamic-discovery pattern as flow_c_full.
const DEFAULT_DETH_FAUCET_HEX: &str = "0xa095d9b3831e96206ff70c2218a6a9";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,miden_client=warn,hyper=warn,sqlx=warn".into()),
        )
        .init();

    let store_path = std::env::var("DARWIN_RELAY_V2_STORE")
        .unwrap_or_else(|_| "./relay-v2.sqlite".to_string());
    let miden_store = std::env::var("DARWIN_RELAY_V2_MIDEN_STORE")
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            format!("{home}/.miden/store.sqlite3")
        });
    let miden_keystore = std::env::var("DARWIN_RELAY_V2_MIDEN_KEYSTORE")
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            format!("{home}/.miden/keystore")
        });
    let relay_wallet_hex = std::env::var("DARWIN_RELAY_V2_RELAY_WALLET_HEX")
        .unwrap_or_else(|_| DEFAULT_RELAY_WALLET_HEX.to_string());
    let controller_hex = std::env::var("DARWIN_RELAY_V2_CONTROLLER_HEX")
        .unwrap_or_else(|_| DEFAULT_CONTROLLER_HEX.to_string());
    let faucet_hex = std::env::var("DARWIN_RELAY_V2_FAUCET_HEX")
        .unwrap_or_else(|_| DEFAULT_DETH_FAUCET_HEX.to_string());
    let poll_interval_s: u64 = std::env::var("DARWIN_RELAY_V2_WORKER_INTERVAL_S")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    let relay_wallet = AccountId::from_hex(&relay_wallet_hex)?;
    let controller = AccountId::from_hex(&controller_hex)?;
    let deth_faucet = AccountId::from_hex(&faucet_hex)?;

    info!(
        %store_path,
        %miden_store,
        %relay_wallet_hex,
        %controller_hex,
        %faucet_hex,
        poll_interval_s,
        "darwin-relay-v2 worker starting",
    );

    let store = SqliteStore::new(PathBuf::from(&miden_store)).await?;
    let mut client = ClientBuilder::<FilesystemKeyStore>::new()
        .grpc_client(&miden_client::rpc::Endpoint::testnet(), None)
        .store(Arc::new(store))
        .filesystem_keystore(PathBuf::from(&miden_keystore))?
        .build()
        .await?;

    // Build the NoteScript once — same assembly the protocol crate
    // uses, vendored locally.
    let core_lib = CoreLibrary::default();
    let sm: Arc<dyn miden_assembly::SourceManager> = Arc::new(DefaultSourceManager::default());
    let math_module = Module::parser(ModuleKind::Library)
        .parse_str(MidenPath::new("darwin::math"), MATH_MASM, sm.clone())
        .map_err(|e| anyhow::anyhow!("parse math.masm: {e}"))?;
    let math_lib = Assembler::default()
        .with_static_library(core_lib.as_ref())
        .map_err(|e| anyhow::anyhow!("attach core_lib: {e}"))?
        .assemble_library([math_module])
        .map_err(|e| anyhow::anyhow!("assemble math.masm: {e}"))?;
    let program = TransactionKernel::assembler()
        .with_static_library(math_lib.as_ref())
        .map_err(|e| anyhow::anyhow!("attach math_lib: {e}"))?
        .assemble_program(ATOMIC_DEPOSIT_NOTE_MASM)
        .map_err(|e| anyhow::anyhow!("assemble atomic_deposit_note.masm: {e}"))?;
    let note_script = NoteScript::new(program);

    loop {
        if let Err(e) = tick(
            &mut client,
            &store_path,
            relay_wallet,
            controller,
            deth_faucet,
            &note_script,
        )
        .await
        {
            error!(error = %e, "tick failed");
        }
        tokio::time::sleep(Duration::from_secs(poll_interval_s)).await;
    }
}

async fn tick(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    relay_store_path: &str,
    relay_wallet: AccountId,
    controller: AccountId,
    deth_faucet: AccountId,
    note_script: &NoteScript,
) -> Result<()> {
    info!("syncing miden-client state…");
    client.sync_state().await.context("sync_state")?;

    let pending = load_pending_intents(relay_store_path)?;
    if pending.is_empty() {
        info!("no pending intents — idle");
        return Ok(());
    }
    info!(count = pending.len(), "intents needing on-chain submission");

    // Vault snapshot.
    let relay_acct = client
        .get_account(relay_wallet)
        .await?
        .with_context(|| format!("relay wallet {} not in store", relay_wallet.to_hex()))?;
    let mut vault_balance = relay_acct
        .vault()
        .get_balance(deth_faucet)
        .unwrap_or(0);
    info!(
        relay_wallet = %relay_wallet.to_hex(),
        faucet = %deth_faucet.to_hex(),
        balance = vault_balance,
        "relay vault snapshot",
    );

    for intent in pending {
        let amount = match intent.amount_in_wei.parse::<u64>() {
            Ok(a) => a,
            Err(_) => {
                warn!(
                    correlation_id = %intent.correlation_id,
                    amount = %intent.amount_in_wei,
                    "amount doesn't fit in u64 — skipping",
                );
                mark_intent_error(
                    relay_store_path,
                    &intent.correlation_id,
                    "amount overflows u64",
                )?;
                continue;
            }
        };
        if vault_balance < amount {
            info!(
                correlation_id = %intent.correlation_id,
                need = amount,
                have = vault_balance,
                "vault under-funded — leaving for next tick",
            );
            continue;
        }

        match submit_atomic_deposit(
            client,
            relay_wallet,
            controller,
            deth_faucet,
            amount,
            note_script,
            &intent,
        )
        .await
        {
            Ok((tx_hex, note_hex)) => {
                info!(
                    correlation_id = %intent.correlation_id,
                    atomic_deposit_tx = %tx_hex,
                    note_id = %note_hex,
                    "atomic_deposit_note submitted",
                );
                mark_intent_submitted(
                    relay_store_path,
                    &intent.correlation_id,
                    &tx_hex,
                    &note_hex,
                )?;
                vault_balance -= amount;
            }
            Err(e) => {
                error!(
                    correlation_id = %intent.correlation_id,
                    error = %e,
                    "atomic_deposit submission failed",
                );
                mark_intent_error(
                    relay_store_path,
                    &intent.correlation_id,
                    &format!("submission failed: {e}"),
                )?;
            }
        }
    }
    Ok(())
}

async fn submit_atomic_deposit(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    relay_wallet: AccountId,
    controller: AccountId,
    deth_faucet: AccountId,
    amount: u64,
    note_script: &NoteScript,
    intent: &PendingIntent,
) -> Result<(String, String)> {
    let assets = NoteAssets::new(vec![Asset::Fungible(FungibleAsset::new(
        deth_faucet,
        amount,
    )?)])?;
    // Tag the note with the controller so the consumer side picks it
    // up; same shape as flow_a_full.
    let _ = controller; // sender is metadata-only here; controller consumes via input_notes when it picks the note up. The note carries the assets which the controller's receive_asset will absorb.
    let metadata = NoteMetadata::new(relay_wallet, NoteType::Public);

    let mut serial_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut serial_bytes);
    let serial_num = miden_client::Word::try_from(
        serial_bytes
            .chunks_exact(8)
            .map(|chunk| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(chunk);
                miden_client::Felt::new(u64::from_le_bytes(buf))
            })
            .collect::<Vec<_>>()
            .as_slice(),
    )?;

    // Storage felts shape per atomic_deposit_note.masm:
    //   [deposit_value, fee_factor, nav_scale]
    // For the M3 demo we credit 99.7% (30bps mint fee) and keep
    // nav_scale = deposit_value so basket_amount_minted ≈ amount * 0.997.
    let nav_scale: u64 = amount;
    let storage_felts = vec![
        miden_client::Felt::new(amount),
        miden_client::Felt::new(9_970),
        miden_client::Felt::new(nav_scale),
    ];
    let recipient = NoteRecipient::new(
        serial_num,
        note_script.clone(),
        NoteStorage::new(storage_felts)?,
    );
    let note = Note::new(assets, metadata, recipient);
    let note_id = note.id();

    let req = TransactionRequestBuilder::new()
        .own_output_notes(vec![note.clone()])
        .build()?;
    let result = client.execute_transaction(relay_wallet, req).await?;
    let tx_id = result.executed_transaction().id();
    let prover = client.prover();
    let proven = client.prove_transaction_with(&result, prover).await?;
    let height = client.submit_proven_transaction(proven, &result).await?;
    client.apply_transaction(&result, height).await?;

    info!(
        correlation_id = %intent.correlation_id,
        height = %height,
        "atomic_deposit tx confirmed",
    );
    Ok((format!("{tx_id}"), format!("{note_id}")))
}

// ---------------------------------------------------------------------------
// SQLite glue against the v2 service's store.
// ---------------------------------------------------------------------------

struct PendingIntent {
    correlation_id: String,
    user_evm_addr: String,
    basket_symbol: String,
    amount_in_wei: String,
}

fn load_pending_intents(store_path: &str) -> Result<Vec<PendingIntent>> {
    let conn = Connection::open(store_path)?;
    let mut stmt = conn.prepare(
        r#"SELECT correlation_id, user_evm_addr, basket_symbol, amount_in_wei
              FROM intents
              WHERE stage = 'POSITION_CREDITED'
                AND atomic_deposit_tx IS NULL
              ORDER BY created_at ASC"#,
    )?;
    let mut rows = stmt.query(params![])?;
    let mut out = vec![];
    while let Some(r) = rows.next()? {
        out.push(PendingIntent {
            correlation_id: r.get(0)?,
            user_evm_addr: r.get(1)?,
            basket_symbol: r.get(2)?,
            amount_in_wei: r.get(3)?,
        });
    }
    Ok(out)
}

fn mark_intent_submitted(
    store_path: &str,
    correlation_id: &str,
    tx_hex: &str,
    note_hex: &str,
) -> Result<()> {
    let conn = Connection::open(store_path)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    conn.execute(
        r#"UPDATE intents
              SET atomic_deposit_tx = ?2,
                  miden_consume_tx  = ?3,
                  updated_at        = ?4
              WHERE correlation_id = ?1"#,
        params![correlation_id, tx_hex, note_hex, now],
    )?;
    Ok(())
}

fn mark_intent_error(
    store_path: &str,
    correlation_id: &str,
    msg: &str,
) -> Result<()> {
    let conn = Connection::open(store_path)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    conn.execute(
        r#"UPDATE intents
              SET error      = ?2,
                  updated_at = ?3
              WHERE correlation_id = ?1"#,
        params![correlation_id, msg, now],
    )?;
    Ok(())
}
