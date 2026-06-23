//! Miden-native deposit E2E test (CLI).
//!
//! Replicates the on-chain mechanism the frontend's MidenDepositPanel
//! exercises in the browser: a user's own Miden wallet signs an emit
//! tx for an atomic_deposit_note_v2 carrying dETH, then the basket
//! controller consumes it and credits slot-10 at the user's
//! per-(user, basket) key.
//!
//! Run from CLI to validate the Miden-native path end-to-end without a
//! MidenFi browser extension. Reuses the relay's own MASM v2 + math
//! library + controller hex, so the only difference vs the Sepolia
//! path is *which* AccountId signs the emit tx and what felts land in
//! the slot-10 key (Miden AccountId.suffix/prefix.as_felt instead of
//! evmToUserIdFelts).
//!
//! Caveats:
//!   * Needs the SENDER's Falcon key in ~/.miden/keystore.
//!   * Needs the CONTROLLER's key in the same keystore (the relay-op
//!     setup already has both, since the worker uses them).
//!   * Defaults are pinned to the post-reseed-2026-06-08 bridge mock
//!     IDs; the dETH faucet hex re-rolls on every reseed (see
//!     `mock_bridge_faucet_drift` memory) so override via
//!     `DARWIN_DETH_FAUCET_HEX` env when stale.
//!
//! Usage:
//!     cargo run --release --bin miden_native_deposit_test \
//!         --features v2-worker -- \
//!         --sender 0x6651e5ac8195b2000187593fff7e03 \
//!         --basket DCC \
//!         --amount 1000

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use miden_assembly::ast::{Module, ModuleKind};
use miden_assembly::{Assembler, DefaultSourceManager, Path as MidenPath};
use miden_client::account::AccountId;
use miden_client::asset::{Asset, FungibleAsset};
use miden_client::builder::ClientBuilder;
use miden_client::keystore::FilesystemKeyStore;
use miden_client::note::{
    Note, NoteAssets, NoteRecipient, NoteScript, NoteStorage, NoteType,
};
use miden_client::transaction::TransactionRequestBuilder;
use miden_client_sqlite_store::SqliteStore;
use miden_core_lib::CoreLibrary;
use miden_protocol::transaction::TransactionKernel;
use rand::RngCore;
use serde::Deserialize;

// MASM vendored from the relay's own asm/ — identical to what the
// worker uses for ETH-user deposits and identical to what the frontend
// fetches from public/notes/atomic_deposit_note_v2.masm. Keeps the
// Miden-native path bit-identical to the Sepolia path at the kernel.
const ATOMIC_DEPOSIT_NOTE_V2_MASM: &str =
    include_str!("../../asm/atomic_deposit_note_v2.masm");
const MATH_MASM: &str = include_str!("../../asm/lib/math.masm");

const DEFAULT_CONTROLLER_HEX: &str = "0xbef7d2e89e9c3e006e10f959fa16d2";
// dETH faucet post 2026-06-08 bridge reseed. Override via env when the
// mock is reseeded again.
const DEFAULT_DETH_FAUCET_HEX: &str = "0x3063d8a9be0394a074c806d86ab776";

fn basket_faucet_hex(symbol: &str) -> Option<&'static str> {
    // Mirror of darwin-frontend/src/components/MidenDepositPanel.tsx
    // and darwin-relay/src/bin/darwin_relay_v2_worker.rs.
    match symbol {
        "DCC" => Some("0x2066f2da1f91ba202af5251d39101c"),
        "DAG" => Some("0xfb6811fd6399df206d44f62800620d"),
        "DCO" => Some("0xbe4efc6729eb3220423b7d6d6a0942"),
        _ => None,
    }
}

#[derive(Deserialize, Debug, Clone)]
struct PricesResponse {
    eth: f64,
    wbtc: f64,
    usdt: f64,
    dai: f64,
}

async fn fetch_prices() -> Result<PricesResponse> {
    let url = std::env::var("DARWIN_PRICES_URL")
        .unwrap_or_else(|_| "https://darwin.market/api/prices".to_string());
    let body: PricesResponse = reqwest::get(&url)
        .await
        .with_context(|| format!("GET {url}"))?
        .error_for_status()
        .with_context(|| format!("GET {url} non-2xx"))?
        .json()
        .await
        .with_context(|| "decode /api/prices")?;
    Ok(body)
}

fn basket_nav_usd(symbol: &str, p: &PricesResponse) -> Option<f64> {
    // Same weights as darwin-frontend/src/lib/baskets.ts.
    match symbol {
        "DCC" => Some(0.40 * p.wbtc + 0.40 * p.eth + 0.20 * p.usdt),
        "DAG" => Some(0.50 * p.wbtc + 0.50 * p.eth),
        "DCO" => Some(0.10 * p.wbtc + 0.10 * p.eth + 0.40 * p.usdt + 0.40 * p.dai),
        _ => None,
    }
}

const ASSET_DECIMALS_DETH: i32 = 8;
const BASKET_TOKEN_DECIMALS: i32 = 8;

fn compute_storage_math(
    amount: u64,
    asset_price_usd: f64,
    basket_nav_usd: f64,
) -> (u64, u64, u64, u64) {
    let fee_factor: u64 = 9_970;
    let asset_price_round = asset_price_usd.round().max(1.0) as u64;
    let deposit_value = amount.saturating_mul(asset_price_round).max(1);
    let nav_flat: u64 = basket_nav_usd.round().max(1.0) as u64;
    let mut nav_scale = nav_flat.saturating_mul(10_000);
    let basket_minus_asset_dec = BASKET_TOKEN_DECIMALS - ASSET_DECIMALS_DETH;
    if basket_minus_asset_dec > 0 {
        nav_scale = (nav_scale / 10u64.pow(basket_minus_asset_dec as u32)).max(1);
    } else if basket_minus_asset_dec < 0 {
        nav_scale = nav_scale.saturating_mul(10u64.pow((-basket_minus_asset_dec) as u32));
    }
    let expected_mint = ((deposit_value as u128).saturating_mul(fee_factor as u128)
        / (nav_scale.max(1) as u128))
        .min(u64::MAX as u128) as u64;
    (deposit_value, fee_factor, nav_scale, expected_mint)
}

fn cli_arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1).cloned())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,miden_client=warn")),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    let sender_hex = cli_arg(&args, "--sender")
        .context("--sender <hex> required (Miden AccountId of user wallet)")?;
    let basket = cli_arg(&args, "--basket")
        .context("--basket <DCC|DAG|DCO> required")?
        .to_uppercase();
    let amount: u64 = cli_arg(&args, "--amount")
        .context("--amount <base_units, u64> required")?
        .parse()
        .context("--amount must be a u64")?;

    let controller_hex = std::env::var("DARWIN_CONTROLLER_HEX")
        .unwrap_or_else(|_| DEFAULT_CONTROLLER_HEX.to_string());
    let deth_faucet_hex = std::env::var("DARWIN_DETH_FAUCET_HEX")
        .unwrap_or_else(|_| DEFAULT_DETH_FAUCET_HEX.to_string());

    let home = std::env::var("HOME")?;
    let store_path: PathBuf = std::env::var("DARWIN_MIDEN_STORE")
        .unwrap_or_else(|_| format!("{home}/.miden/store.sqlite3"))
        .into();
    let keystore_path: PathBuf = std::env::var("DARWIN_MIDEN_KEYSTORE")
        .unwrap_or_else(|_| format!("{home}/.miden/keystore"))
        .into();

    println!("─── Miden-native deposit test ───");
    println!("  sender:    {sender_hex}");
    println!("  basket:    {basket}");
    println!("  amount:    {amount} (dETH base units = {} dETH)", amount as f64 / 1e8);
    println!("  controller:{controller_hex}");
    println!("  faucet:    {deth_faucet_hex}");
    println!();

    let endpoint = match std::env::var("MIDEN_NETWORK")
        .ok()
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("devnet") => miden_client::rpc::Endpoint::devnet(),
        Some("localhost") | Some("local") => miden_client::rpc::Endpoint::localhost(),
        _ => miden_client::rpc::Endpoint::testnet(),
    };
    println!("Connecting to Miden ({endpoint:?})…");
    let store = SqliteStore::new(store_path).await?;
    let mut client = ClientBuilder::<FilesystemKeyStore>::new()
        .grpc_client(&endpoint, None)
        .store(Arc::new(store))
        .filesystem_keystore(keystore_path)?
        .build()
        .await?;

    println!("Sync state…");
    let sync = client.sync_state().await?;
    println!("  synced to block {}", sync.block_num);

    let sender = AccountId::from_hex(&sender_hex)?;
    let controller = AccountId::from_hex(&controller_hex)?;
    let deth_faucet = AccountId::from_hex(&deth_faucet_hex)?;
    let basket_faucet_str = basket_faucet_hex(&basket)
        .with_context(|| format!("unknown basket symbol {basket}"))?;
    let basket_faucet = AccountId::from_hex(basket_faucet_str)?;

    // Vault sanity check
    let sender_acct = client
        .get_account(sender)
        .await?
        .with_context(|| format!("sender {sender_hex} not in store; import it first"))?;
    // v0.15: vault.get_balance takes AssetVaultKey, returns
    // Result<AssetAmount>. Build the key via FungibleAsset and convert
    // back to u64 at the boundary.
    let sender_balance: u64 = miden_client::asset::FungibleAsset::new(deth_faucet, 0)
        .map(|fa| fa.vault_key())
        .ok()
        .and_then(|k| sender_acct.vault().get_balance(k).ok())
        .map(u64::from)
        .unwrap_or(0);
    if sender_balance < amount {
        anyhow::bail!(
            "sender vault has {sender_balance} dETH from faucet {deth_faucet_hex}, \
             need {amount}; mint more from the bridge mock first"
        );
    }
    println!("Sender vault: {sender_balance} dETH from {deth_faucet_hex} (sufficient)");
    println!();

    println!("Compiling atomic_deposit_note_v2.masm …");
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
        .with_static_library(core_lib.as_ref())
        .map_err(|e| anyhow::anyhow!("attach core_lib (deposit v2): {e}"))?
        .with_static_library(math_lib.as_ref())
        .map_err(|e| anyhow::anyhow!("attach math_lib: {e}"))?
        .assemble_program(ATOMIC_DEPOSIT_NOTE_V2_MASM)
        .map_err(|e| anyhow::anyhow!("assemble note v2: {e}"))?;
    let note_script = NoteScript::new(program);

    println!("Fetching live prices…");
    let prices = fetch_prices().await?;
    let nav = basket_nav_usd(&basket, &prices)
        .with_context(|| format!("no NAV mapping for {basket}"))?;
    let (deposit_value, fee_factor, nav_scale, expected_mint) =
        compute_storage_math(amount, prices.eth, nav);
    println!(
        "  eth=${:.2}  basket_nav=${:.2}",
        prices.eth, nav
    );
    println!(
        "  storage felts: deposit_value={deposit_value} fee_factor={fee_factor} \
         nav_scale={nav_scale}",
    );
    println!("  expected slot-10 mint = {expected_mint} basket-token base units");
    println!();

    // Build the 7-felt storage layout the v2 note expects.
    let user_suffix = sender.suffix();
    let user_prefix = sender.prefix().as_felt();
    let basket_suffix = basket_faucet.suffix();
    let basket_prefix = basket_faucet.prefix().as_felt();
    let storage_felts = vec![
        miden_client::Felt::new(deposit_value).expect("bounded by NAV math"),
        miden_client::Felt::new(fee_factor).expect("bounded by NAV math"),
        miden_client::Felt::new(nav_scale).expect("bounded by NAV math"),
        user_suffix,
        user_prefix,
        basket_suffix,
        basket_prefix,
    ];

    let mut serial_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut serial_bytes);
    let serial_num = miden_client::Word::try_from(
        serial_bytes
            .chunks_exact(8)
            .map(|chunk| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(chunk);
                miden_client::Felt::new(u64::from_le_bytes(buf) & 0xFFFF_FFFE_FFFF_FFFF).expect("masked to Goldilocks safe range")
            })
            .collect::<Vec<_>>()
            .as_slice(),
    )?;

    let assets = NoteAssets::new(vec![Asset::Fungible(FungibleAsset::new(
        deth_faucet,
        amount,
    )?)])?;
    let metadata = miden_client::note::PartialNoteMetadata::new(sender, NoteType::Public);
    let recipient = NoteRecipient::new(
        serial_num,
        note_script.clone(),
        NoteStorage::new(storage_felts)?,
    );
    let note = Note::new(assets, metadata, recipient);
    let note_id = note.id();

    println!("Step 1/2: Sender emits atomic_deposit_note_v2…");
    let emit_req = TransactionRequestBuilder::new()
        .own_output_notes(vec![note.clone()])
        .build()?;
    let emit_result = client.execute_transaction(sender, emit_req).await?;
    let emit_tx_id = emit_result.executed_transaction().id();
    let prover = client.prover();
    let emit_proven = client
        .prove_transaction_with(&emit_result, prover.clone())
        .await?;
    let emit_height = client
        .submit_proven_transaction(emit_proven, &emit_result)
        .await?;
    client.apply_transaction(&emit_result, emit_height).await?;
    println!("  emit tx:  {emit_tx_id}");
    println!("  note id:  {note_id}");
    println!("  height:   {emit_height}");
    println!();

    println!("Step 2/2: Controller consumes the note (slot-10 credit)…");
    let consume_req = TransactionRequestBuilder::new()
        .input_notes(vec![(note.clone(), None)])
        .build()?;
    let consume_result = client
        .execute_transaction(controller, consume_req)
        .await
        .context(
            "controller consume failed — needs controller's keys in keystore (the relay-op \
             setup has them; if running on a fresh box, restore the relay backup)",
        )?;
    let consume_tx_id = consume_result.executed_transaction().id();
    let consume_proven = client
        .prove_transaction_with(&consume_result, prover)
        .await?;
    let consume_height = client
        .submit_proven_transaction(consume_proven, &consume_result)
        .await?;
    client
        .apply_transaction(&consume_result, consume_height)
        .await?;
    println!("  consume tx: {consume_tx_id}");
    println!("  height:     {consume_height}");
    println!();

    println!("─── Slot-10 key for verification ───");
    println!("  user_suffix:   {user_suffix}");
    println!("  user_prefix:   {user_prefix}");
    println!("  basket_suffix: {basket_suffix}");
    println!("  basket_prefix: {basket_prefix}");
    println!();
    println!("Verify via /api/position:");
    println!(
        "  curl -s -X POST https://darwin.market/api/position \\\n    \
         -H 'Content-Type: application/json' \\\n    \
         -d '{{\"suffix\":\"{user_suffix}\",\"prefix\":\"{user_prefix}\",\
         \"basketSuffix\":\"{basket_suffix}\",\"basketPrefix\":\"{basket_prefix}\"}}'"
    );
    println!();
    println!("expected delta on slot-10: +{expected_mint} basket-token base units");
    Ok(())
}
