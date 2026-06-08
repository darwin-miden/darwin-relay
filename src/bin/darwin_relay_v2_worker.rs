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
    Note, NoteAssets, NoteAttachment, NoteMetadata, NoteRecipient, NoteScript, NoteStorage,
    NoteTag, NoteType,
};
use miden_client::transaction::TransactionRequestBuilder;
use miden_client_sqlite_store::SqliteStore;

// Canonical Bali AggLayer note format. The B2AggNote::create call
// returns a Note whose script_root matches what the L1 bridge
// expects, so we don't have to vendor the MASM ourselves — the
// crate keeps it pinned to the consensus layer.
use miden_base_agglayer::{B2AggNote, EthAddress};
use miden_core_lib::CoreLibrary;
use miden_protocol::transaction::TransactionKernel;
use rand::RngCore;
use serde::{Deserialize, Serialize};

// MASM sources vendored from darwin-protocol/crates/darwin-notes and
// darwin-protocol-account. Kept in sync manually; both repos pin the
// same miden-assembly 0.22 / miden-protocol 0.14 ABI.
const ATOMIC_DEPOSIT_NOTE_MASM: &str = include_str!("../../asm/atomic_deposit_note.masm");
const ATOMIC_DEPOSIT_NOTE_V2_MASM: &str =
    include_str!("../../asm/atomic_deposit_note_v2.masm");

// Basket token faucet IDs on Miden testnet. The atomic_deposit_note_v2
// script writes slot-10 at the key (user_suffix, user_prefix,
// basket_suffix, basket_prefix); the frontend reads at the SAME key.
// Without these, the worker pushed only 5 storage felts and the script
// read basket_suffix/basket_prefix as 0/0 — depositing landed in a slot
// the frontend never queries (silent zero-balance).
//
// Mirrors the frontend's BASKET_TOKEN_FAUCET map in
// src/components/MidenDepositPanel.tsx. Both must stay in sync.
fn basket_faucet_hex(symbol: &str) -> Option<&'static str> {
    match symbol {
        "DCC" => Some("0x2066f2da1f91ba202af5251d39101c"),
        "DAG" => Some("0xfb6811fd6399df206d44f62800620d"),
        "DCO" => Some("0xbe4efc6729eb3220423b7d6d6a0942"),
        _ => None,
    }
}

// Both the deposit asset (dETH, from the 1Click bridge) and the basket
// tokens (DCC/DAG/DCO) are 8-decimal on Miden — so the felt scaling
// term in `nav_scale = nav * 10000 / 10^(basket_dec - asset_dec)`
// collapses to `nav * 10000`. When other assets bridge in (USDT 6-dec,
// WBTC 8-dec), pass `asset_decimals` through the intent and reinstate
// the per-decimal scaling branch that the frontend has in
// `computeStorageFelts` (MidenDepositPanel.tsx).
const ASSET_DECIMALS_DETH: i32 = 8;
const BASKET_TOKEN_DECIMALS: i32 = 8;

// Live constituent prices fetched from the Vercel-side /api/prices.
// Same shape as the frontend's `PricesResponse` in lib/prices.ts.
// Used to derive the per-basket NAV the note script needs to mint the
// correct slot-10 credit (slot-10 is denominated in basket-token base
// units, not deposit-asset base units).
#[derive(Deserialize, Debug, Clone)]
struct PricesResponse {
    eth: f64,
    wbtc: f64,
    usdt: f64,
    dai: f64,
}

async fn fetch_prices(http: &reqwest::Client) -> Result<PricesResponse> {
    let url = std::env::var("DARWIN_RELAY_V2_PRICES_URL")
        .unwrap_or_else(|_| "https://darwin.market/api/prices".to_string());
    let resp = http
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?
        .error_for_status()
        .with_context(|| format!("GET {url} non-2xx"))?;
    let body: PricesResponse = resp
        .json()
        .await
        .with_context(|| format!("decode {url} as PricesResponse"))?;
    Ok(body)
}

// Weighted NAV per basket unit. Mirrors `basketNav()` in
// darwin-frontend/src/lib/prices.ts — weights MUST stay in sync with
// the catalogue in darwin-frontend/src/lib/baskets.ts.
fn basket_nav_usd(symbol: &str, p: &PricesResponse) -> Option<f64> {
    match symbol {
        "DCC" => Some(0.40 * p.wbtc + 0.40 * p.eth + 0.20 * p.usdt),
        "DAG" => Some(0.50 * p.wbtc + 0.50 * p.eth),
        "DCO" => Some(0.10 * p.wbtc + 0.10 * p.eth + 0.40 * p.usdt + 0.40 * p.dai),
        _ => None,
    }
}
const ATOMIC_REDEEM_NOTE_MASM: &str = include_str!("../../asm/atomic_redeem_note.masm");
const MATH_MASM: &str = include_str!("../../asm/lib/math.masm");
const BRIDGE_OUT_V1_MASM: &str = include_str!("../../asm/bridge_out_v1.masm");

const DEFAULT_RELAY_WALLET_HEX: &str = "0xed3cd5befa3207805f8529207cfc0d";

// Tunables read once from env at first access. Magic numbers that
// used to be scattered through the call sites (decimal scaling, fee
// factor, redeem-fee net basis points) live here so future fee changes
// or non-1:1 USD pegs are a config flip, not a code change. Defaults
// match what shipped with M3.
struct Tunables {
    /// EVM 18-dec → Miden 8-dec base unit (default 10^10).
    wei_per_miden_base: u128,
    /// fee_factor felt for atomic_deposit_note_v2 (default 10_000).
    fee_factor: u64,
    /// 1 - redeem_fee in 10_000ths. Default 9_970 = 30 bps redeem fee.
    /// Used by the outbound legs to compute the released-underlying
    /// amount, and by the status poller to match Bali claims to
    /// in-flight redemptions.
    redeem_fee_net_bps: u64,
}

fn tunables() -> &'static Tunables {
    use std::sync::OnceLock;
    static T: OnceLock<Tunables> = OnceLock::new();
    T.get_or_init(|| {
        let t = Tunables {
            wei_per_miden_base: std::env::var("DARWIN_RELAY_V2_WEI_PER_MIDEN_BASE")
                .ok().and_then(|s| s.parse().ok())
                .unwrap_or(10_000_000_000),
            fee_factor: std::env::var("DARWIN_RELAY_V2_FEE_FACTOR")
                .ok().and_then(|s| s.parse().ok())
                .unwrap_or(10_000),
            redeem_fee_net_bps: std::env::var("DARWIN_RELAY_V2_REDEEM_FEE_NET_BPS")
                .ok().and_then(|s| s.parse().ok())
                .unwrap_or(9_970),
        };
        info!(
            wei_per_miden_base = t.wei_per_miden_base,
            fee_factor = t.fee_factor,
            redeem_fee_net_bps = t.redeem_fee_net_bps,
            "tunables loaded",
        );
        t
    })
}
// v6 controller — deployed 2026-05-26, strict superset of v5 (adds
// slot 11 fee_recipient + receive_and_credit). For the notes this
// worker emits (receive_asset + set_user_position), the MAST roots
// are byte-identical to v5; only the get_* read procs differ. Older
// deploys can still be targeted via the CLI override.
const DEFAULT_CONTROLLER_HEX: &str = "0x2a3ea0a268d97b80497d6a966e3141";

// "Native" controller — the controller the frontend targets when a
// user emits a deposit note directly from their Miden wallet (no
// relay hop, no 1Click). Same controller as the relay-driven path
// (v6 fee-routing) because the bare v2 "real-bodies" controller
// lacks slot-10 (per-user position map) and slot-11 (fee recipient),
// so notes hitting it would leave assets in the aggregate vault but
// never credit a user position. By targeting v6, user-emitted notes
// accumulate into the same on-chain ledger as relay-emitted ones and
// the portfolio UI reads a single source of truth. This worker
// watches v6 for inbound user-emitted notes and runs the consume tx
// that credits the user's slot — independently of `process_deposits`
// which only handles relay-tracked intents. Set the env var to the
// empty string to disable native-side polling.
const DEFAULT_NATIVE_CONTROLLER_HEX: &str = "0x2a3ea0a268d97b80497d6a966e3141";
// Miden testnet dETH faucet (the M1 deth-equivalent fungible faucet
// the basket controllers know about). The 1Click bridge mints
// `miden-testnet:eth` from a single faucet at runtime — the relay
// vault snapshot during deposit processing must check the balance
// under THAT faucet id, otherwise it reads a stale residual from an
// earlier faucet generation and refuses to credit any deposit.
//
// Source-of-truth = the live `faucet_account_id` field on the
// bridge's `submitting Miden pay-to-id mint` log lines. Override via
// env var when the bridge instance re-rolls (fresh seed → fresh
// faucet account → this hardcoded value goes stale).
const DEFAULT_DETH_FAUCET_HEX: &str = "0xf45bad08699050a02d5db52d4e1c28";

// 1Click bridge's miden-testnet:eth faucet — what the bridge mints
// inbound and what it expects on outbound. The relay vault must hold
// this faucet's tokens for the outbound P2ID leg to actually carry
// usable assets. Same caveat as DEFAULT_DETH_FAUCET_HEX above:
// regenerated when the bridge container is reseeded, so re-pin via
// env on faucet drift.
const DEFAULT_ONECLICK_FAUCET_HEX: &str = "0xf45bad08699050a02d5db52d4e1c28";
const DEFAULT_ONECLICK_URL: &str = "http://localhost:8080";

// Canonical Bali AggLayer (gateway-fm) — used by the B2AGG outbound
// path. Different from the 1Click constants above: the Bali bridge
// account + faucet are operated by gateway-fm, the L1↔L2 round-trip
// is permissionless (proof-based on Sepolia), and the relay does
// not need to involve a 1Click solver.
const DEFAULT_BALI_BRIDGE_HEX: &str = "0xc98bb07c188cd2500e13f68a069cdc";
const DEFAULT_BALI_FAUCET_HEX: &str = "0xe63ba7bc2c19ff603c52c67fa4426d";
const DEFAULT_BALI_BRIDGE_SERVICE: &str =
    "https://miden-testnet-bridge.dev.eu-north-3.gateway.fm";
// Outbound modes: "b2agg" (default, canonical Bali) or
// "bridge_out_v1" (legacy, Brian's 1Click mock format). Keep the
// legacy path behind a flag for backwards-compat with deployments
// still pointed at the 1Click mock; otherwise default to canonical.
const DEFAULT_OUTBOUND_MODE: &str = "b2agg";
const ONECLICK_ORIGIN_ASSET: &str = "miden-testnet:eth";
const ONECLICK_DEST_ASSET: &str = "eth-sepolia:eth";

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
    // Empty string = disable native-side polling (rollback knob).
    let native_controller_hex = std::env::var("DARWIN_RELAY_V2_NATIVE_CONTROLLER_HEX")
        .unwrap_or_else(|_| DEFAULT_NATIVE_CONTROLLER_HEX.to_string());
    let faucet_hex = std::env::var("DARWIN_RELAY_V2_FAUCET_HEX")
        .unwrap_or_else(|_| DEFAULT_DETH_FAUCET_HEX.to_string());
    let oneclick_faucet_hex = std::env::var("DARWIN_RELAY_V2_OUTBOUND_FAUCET_HEX")
        .unwrap_or_else(|_| DEFAULT_ONECLICK_FAUCET_HEX.to_string());
    let oneclick_url = std::env::var("DARWIN_RELAY_V2_ONECLICK_URL")
        .unwrap_or_else(|_| DEFAULT_ONECLICK_URL.to_string());
    let bali_bridge_hex = std::env::var("DARWIN_RELAY_V2_BALI_BRIDGE_HEX")
        .unwrap_or_else(|_| DEFAULT_BALI_BRIDGE_HEX.to_string());
    let bali_faucet_hex = std::env::var("DARWIN_RELAY_V2_BALI_FAUCET_HEX")
        .unwrap_or_else(|_| DEFAULT_BALI_FAUCET_HEX.to_string());
    let bali_bridge_service = std::env::var("DARWIN_RELAY_V2_BALI_BRIDGE_SERVICE")
        .unwrap_or_else(|_| DEFAULT_BALI_BRIDGE_SERVICE.to_string());
    let outbound_mode = std::env::var("DARWIN_RELAY_V2_OUTBOUND_MODE")
        .unwrap_or_else(|_| DEFAULT_OUTBOUND_MODE.to_string());
    let poll_interval_s: u64 = std::env::var("DARWIN_RELAY_V2_WORKER_INTERVAL_S")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    let relay_wallet = AccountId::from_hex(&relay_wallet_hex)?;
    let controller = AccountId::from_hex(&controller_hex)?;
    let native_controller = if native_controller_hex.trim().is_empty() {
        None
    } else {
        Some(AccountId::from_hex(&native_controller_hex)?)
    };
    // NoteTag::with_account_target on the native controller — used as
    // a filter signal in both directions:
    //   1. drain_native_controller scans consumable notes (any account)
    //      and keeps only those whose metadata tag == native_tag, since
    //      miden-client's get_consumable_notes(controller) doesn't
    //      reliably surface user-emitted custom-script notes for the
    //      controller alone.
    //   2. drain_inbound_notes (relay wallet) skips notes carrying
    //      native_tag so the relay doesn't race the controller and
    //      win — that would land the asset in the relay vault instead
    //      of the controller vault.
    // None when native polling is disabled.
    let native_tag: Option<u32> =
        native_controller.map(|id| NoteTag::with_account_target(id).as_u32());
    let deth_faucet = AccountId::from_hex(&faucet_hex)?;
    let oneclick_faucet = AccountId::from_hex(&oneclick_faucet_hex)?;
    let bali_bridge = AccountId::from_hex(&bali_bridge_hex)?;
    let bali_faucet = AccountId::from_hex(&bali_faucet_hex)?;

    info!(
        %store_path,
        %miden_store,
        %relay_wallet_hex,
        %controller_hex,
        native_controller = %native_controller_hex,
        native_tag = ?native_tag,
        %faucet_hex,
        %oneclick_faucet_hex,
        %oneclick_url,
        %bali_bridge_hex,
        %bali_faucet_hex,
        %outbound_mode,
        poll_interval_s,
        "darwin-relay-v2 worker starting",
    );

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;

    let store = SqliteStore::new(PathBuf::from(&miden_store)).await?;
    let mut client = ClientBuilder::<FilesystemKeyStore>::new()
        .grpc_client(&miden_client::rpc::Endpoint::testnet(), None)
        .store(Arc::new(store))
        .filesystem_keystore(PathBuf::from(&miden_keystore))?
        .build()
        .await?;

    // Build the two NoteScripts once — atomic_deposit (for the inbound
    // leg) and atomic_redeem (for the outbound leg). Both link the
    // shared darwin::math library exactly like the protocol crate's
    // flow_a / flow_c do.
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
    // Switch to v2 deposit note when `DARWIN_RELAY_V2_USE_V2_NOTE=1`
    // is set (default: 1, since we have the v5 controller deployed
    // today). v2 emits an extra `set_user_position` call on the v5
    // controller after `receive_asset`, populating slot-10 with the
    // user's per-basket balance — so the frontend's portfolio panel
    // can read positions on-chain instead of trusting the relay db.
    let use_v2_note = std::env::var("DARWIN_RELAY_V2_USE_V2_NOTE")
        .map(|v| v != "0")
        .unwrap_or(true);
    let deposit_program = if use_v2_note {
        // core_lib is attached so the v2 note can `use miden::core::sys`
        // (truncate_stack) to normalise the stack after the FPI read in
        // write_user_position.
        TransactionKernel::assembler()
            .with_static_library(core_lib.as_ref())
            .map_err(|e| anyhow::anyhow!("attach core_lib (deposit_v2): {e}"))?
            .with_static_library(math_lib.as_ref())
            .map_err(|e| anyhow::anyhow!("attach math_lib (deposit_v2): {e}"))?
            .assemble_program(ATOMIC_DEPOSIT_NOTE_V2_MASM)
            .map_err(|e| anyhow::anyhow!("assemble atomic_deposit_note_v2.masm: {e}"))?
    } else {
        TransactionKernel::assembler()
            .with_static_library(math_lib.as_ref())
            .map_err(|e| anyhow::anyhow!("attach math_lib (deposit_v1): {e}"))?
            .assemble_program(ATOMIC_DEPOSIT_NOTE_MASM)
            .map_err(|e| anyhow::anyhow!("assemble atomic_deposit_note.masm: {e}"))?
    };
    info!(use_v2_note, "deposit note variant selected");
    let redeem_program = TransactionKernel::assembler()
        .with_static_library(math_lib.as_ref())
        .map_err(|e| anyhow::anyhow!("attach math_lib (redeem): {e}"))?
        .assemble_program(ATOMIC_REDEEM_NOTE_MASM)
        .map_err(|e| anyhow::anyhow!("assemble atomic_redeem_note.masm: {e}"))?;
    // Bridge-out-v1 imports miden::standards::wallets::basic, which
    // lives in the miden-standards crate (not the TransactionKernel
    // stdlib path). Use the same CodeBuilder Brian's bridge uses on
    // the mint side so the assembled script root matches byte-for-byte.
    let bridge_out_script = miden_standards::code_builder::CodeBuilder::new()
        .compile_note_script(miden_protocol::assembly::diagnostics::NamedSource::new(
            "bridge::notes::bridge_out_v1",
            BRIDGE_OUT_V1_MASM,
        ))
        .map_err(|e| anyhow::anyhow!("assemble bridge_out_v1.masm: {e}"))?;
    let deposit_script = NoteScript::new(deposit_program);
    let redeem_script = NoteScript::new(redeem_program);

    loop {
        let tick_result = tick(
            &mut client,
            &store_path,
            relay_wallet,
            controller,
            native_controller,
            native_tag,
            deth_faucet,
            oneclick_faucet,
            &oneclick_url,
            bali_bridge,
            bali_faucet,
            &bali_bridge_service,
            &outbound_mode,
            &http,
            &deposit_script,
            &redeem_script,
            &bridge_out_script,
        )
        .await;

        let status = match &tick_result {
            Ok(()) => "ok".to_string(),
            Err(e) => {
                error!(error = format!("{e:#}"), "tick failed");
                // First 200 chars of the error so /v0/worker-health can
                // surface it without a log scrape.
                let s = format!("{e:?}");
                if s.len() > 200 { format!("{}…", &s[..200]) } else { s }
            }
        };
        if let Err(e) = write_heartbeat(&store_path, "main", &status) {
            warn!(error = format!("{e:#}"), "heartbeat write failed");
        }

        tokio::time::sleep(Duration::from_secs(poll_interval_s)).await;
    }
}

fn write_heartbeat(store_path: &str, worker_id: &str, status: &str) -> Result<()> {
    let conn = Connection::open(store_path)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    conn.execute(
        r#"INSERT INTO worker_heartbeats (worker_id, last_tick_at, last_tick_status)
              VALUES (?1, ?2, ?3)
              ON CONFLICT(worker_id) DO UPDATE SET
                 last_tick_at     = excluded.last_tick_at,
                 last_tick_status = excluded.last_tick_status"#,
        params![worker_id, now, status],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn tick(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    relay_store_path: &str,
    relay_wallet: AccountId,
    controller: AccountId,
    native_controller: Option<AccountId>,
    native_tag: Option<u32>,
    deth_faucet: AccountId,
    oneclick_faucet: AccountId,
    oneclick_url: &str,
    bali_bridge: AccountId,
    bali_faucet: AccountId,
    bali_bridge_service: &str,
    outbound_mode: &str,
    http: &reqwest::Client,
    deposit_script: &NoteScript,
    redeem_script: &NoteScript,
    bridge_out_script: &NoteScript,
) -> Result<()> {
    info!("syncing miden-client state…");
    client.sync_state().await.context("sync_state")?;

    // Native-controller drain runs FIRST so that user-emitted Darwin
    // notes (Miden-native deposits) land in the controller vault
    // before the relay drain has a chance to claim them and route
    // the asset into the wrong vault. The relay drain below also
    // filters native_tag out as a belt-and-suspenders measure, but
    // ordering is the primary defence.
    if let Some(nc) = native_controller {
        if let Err(e) = drain_native_controller(client, nc, native_tag).await {
            warn!(error = format!("{e:#}"), "drain_native_controller failed (continuing)");
        }
    }

    // Inbound notes (1Click deliveries) sit in COMMITTED state until
    // someone runs a consume tx against them. Drain them so the
    // relay vault reflects the latest inflows before the other passes
    // snapshot it. Skip anything carrying native_tag — those are
    // user-emitted notes destined for the native controller, not the
    // relay; consuming them here would deposit the asset in the
    // relay vault instead of the controller's.
    if let Err(e) = drain_inbound_notes(client, relay_wallet, native_tag).await {
        warn!(error = format!("{e:#}"), "drain_inbound_notes failed (continuing)");
    }

    process_deposits(
        client,
        http,
        relay_store_path,
        relay_wallet,
        controller,
        deth_faucet,
        deposit_script,
    )
    .await?;

    process_redemptions(
        client,
        relay_store_path,
        relay_wallet,
        controller,
        redeem_script,
    )
    .await?;

    match outbound_mode {
        "b2agg" => {
            process_outbound_b2agg(
                client,
                relay_store_path,
                relay_wallet,
                bali_bridge,
                bali_faucet,
                bali_bridge_service,
                http,
            )
            .await?;
            process_outbound_status_b2agg(
                relay_store_path,
                bali_bridge_service,
                http,
            )
            .await?;
        }
        "bridge_out_v1" => {
            process_outbound(
                client,
                relay_store_path,
                relay_wallet,
                oneclick_faucet,
                oneclick_url,
                http,
                bridge_out_script,
            )
            .await?;
            process_outbound_status(relay_store_path, oneclick_url, http).await?;
        }
        other => {
            warn!(mode = %other, "unknown outbound mode — falling back to b2agg");
            process_outbound_b2agg(
                client,
                relay_store_path,
                relay_wallet,
                bali_bridge,
                bali_faucet,
                bali_bridge_service,
                http,
            )
            .await?;
            process_outbound_status_b2agg(
                relay_store_path,
                bali_bridge_service,
                http,
            )
            .await?;
        }
    }

    // Direct canonical bridge-outs (REST /v0/bridge-out queue). These
    // are independent of the redemption path — they don't burn a
    // basket, they just push Bali ETH from the relay vault to a user-
    // specified Sepolia destination. Same B2AggNote::create primitive
    // as the redemption outbound, different trigger.
    process_direct_bridge_outs(
        client,
        relay_store_path,
        relay_wallet,
        bali_bridge,
        bali_faucet,
    )
    .await?;

    Ok(())
}

async fn drain_inbound_notes(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    relay_wallet: AccountId,
    native_tag: Option<u32>,
) -> Result<()> {
    // Notes that have been failing to execute against the relay
    // wallet repeatedly — usually because their script targets a
    // different account or carries state the relay wallet's vault
    // can't satisfy. We skip them up-front to keep the worker log
    // clean. Comma-separated list of `note_id` hex values via env
    // var, or hardcoded fallback for notes we've already triaged.
    //
    // Today's known-stuck: 0xd7d0c957bec74431a20eeea34cc5f64a6bb9f
    //   2683d966bceece3ef3cc7d765bb — appears as consumable to the
    //   relay wallet but execute_transaction returns "transaction
    //   execution failed" on every tick. Origin unclear; treating
    //   as a stale ghost from an earlier dev iteration.
    let deny_list: std::collections::HashSet<String> = std::env::var("DARWIN_RELAY_V2_INBOUND_DENY")
        .unwrap_or_else(|_| {
            "0xd7d0c957bec74431a20eeea34cc5f64a6bb9f2683d966bceece3ef3cc7d765bb".to_string()
        })
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();

    let consumable = client.get_consumable_notes(Some(relay_wallet)).await?;
    if consumable.is_empty() {
        return Ok(());
    }
    info!(count = consumable.len(), "candidate inbound notes");

    // Consume notes one-by-one. The relay wallet's `consumable` set
    // includes both real inbound P2IDs (1Click deliveries) and any
    // darwin notes the relay itself emitted that happen to be
    // self-relevant. Batching them in one tx makes the whole tx fail
    // when any one fails (e.g. a note whose script call.<controller>
    // can't run from the relay account). Per-note try-and-skip is
    // slower but resilient.
    let mut consumed = 0usize;
    for (rec, _relevance) in consumable {
        let note: Note = match TryInto::<Note>::try_into(rec) {
            Ok(n) => n,
            Err(e) => {
                warn!(error = format!("{e:#}"), "skip: not a consumable Note");
                continue;
            }
        };
        let note_id = note.id();
        let note_id_str = format!("{note_id}").to_lowercase();
        if deny_list.contains(&note_id_str) {
            // Silent skip — these are stale ghosts the operator has
            // already triaged; logging on every tick is just noise.
            continue;
        }
        // Defer notes tagged for the native controller. miden-client
        // lists them as relay-consumable too (custom-script notes
        // surface to every tracked account), but consuming them here
        // would execute the deposit script in the relay's context and
        // park the asset in the relay vault. Leave them for the
        // native drain pass, which executes in the controller's
        // context and lands the asset in the controller vault.
        if let Some(nt) = native_tag {
            if note.metadata().tag().as_u32() == nt {
                continue;
            }
        }
        let req = match TransactionRequestBuilder::new().build_consume_notes(vec![note]) {
            Ok(r) => r,
            Err(e) => {
                warn!(%note_id, error = format!("{e:#}"), "skip: build_consume_notes failed");
                continue;
            }
        };
        let result = match client.execute_transaction(relay_wallet, req).await {
            Ok(r) => r,
            Err(e) => {
                warn!(%note_id, error = format!("{e:#}"), "skip: execute failed");
                continue;
            }
        };
        let tx_id = result.executed_transaction().id();
        let prover = client.prover();
        let proven = match client.prove_transaction_with(&result, prover).await {
            Ok(p) => p,
            Err(e) => {
                warn!(%note_id, error = format!("{e:#}"), "skip: prove failed");
                continue;
            }
        };
        let height = match client.submit_proven_transaction(proven, &result).await {
            Ok(h) => h,
            Err(e) => {
                warn!(%note_id, error = format!("{e:#}"), "skip: submit failed");
                continue;
            }
        };
        if let Err(e) = client.apply_transaction(&result, height).await {
            warn!(%note_id, error = format!("{e:#}"), "apply warning");
        }
        info!(%note_id, %tx_id, %height, "inbound note consumed");
        consumed += 1;
    }
    info!(consumed, "drain pass complete");
    Ok(())
}

// Drain notes targeting the Miden-native real-bodies controller. The
// frontend's MidenDepositPanel emits notes here directly (sender = the
// user's Miden wallet, recipient tag = native controller). With
// NoteType::Public those notes are discoverable by anyone syncing the
// chain; the controller still needs a tx to actually consume each
// one. That's what this pass does.
//
// Mirrors `drain_inbound_notes` but acts on the controller account
// instead of the relay wallet, and reuses the same deny-list env var
// to silence known-stuck ghosts regardless of which account they
// happen to look consumable for.
async fn drain_native_controller(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    native_controller: AccountId,
    native_tag: Option<u32>,
) -> Result<()> {
    let deny_list: std::collections::HashSet<String> = std::env::var("DARWIN_RELAY_V2_INBOUND_DENY")
        .unwrap_or_else(|_| {
            "0xd7d0c957bec74431a20eeea34cc5f64a6bb9f2683d966bceece3ef3cc7d765bb".to_string()
        })
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();

    // Query the union of "consumable for the controller" + "consumable
    // for anyone" because miden-client's get_consumable_notes filter
    // is best-effort: custom-script user notes only show up in the
    // any-account list, never under the specific controller, even
    // though the controller is the only account that can correctly
    // execute them. native_tag is the discriminator we use to
    // recognise them in that broader list.
    let mut by_id: std::collections::HashMap<miden_client::note::NoteId, miden_client::note::Note> =
        std::collections::HashMap::new();
    let primary = client.get_consumable_notes(Some(native_controller)).await?;
    for (rec, _rel) in primary {
        if let Ok(n) = TryInto::<Note>::try_into(rec) {
            by_id.insert(n.id(), n);
        }
    }
    if let Some(nt) = native_tag {
        let anyone = client.get_consumable_notes(None).await?;
        for (rec, _rel) in anyone {
            if let Ok(n) = TryInto::<Note>::try_into(rec) {
                if n.metadata().tag().as_u32() == nt {
                    by_id.entry(n.id()).or_insert(n);
                }
            }
        }
    }
    if by_id.is_empty() {
        return Ok(());
    }
    info!(
        count = by_id.len(),
        native_controller = %native_controller.to_hex(),
        "candidate notes targeting native controller",
    );

    let mut consumed = 0usize;
    for note in by_id.into_values() {
        let note_id = note.id();
        let note_id_str = format!("{note_id}").to_lowercase();
        if deny_list.contains(&note_id_str) {
            continue;
        }
        let req = match TransactionRequestBuilder::new().build_consume_notes(vec![note]) {
            Ok(r) => r,
            Err(e) => {
                warn!(%note_id, error = format!("{e:#}"), "skip: build_consume_notes failed (native)");
                continue;
            }
        };
        let result = match client.execute_transaction(native_controller, req).await {
            Ok(r) => r,
            Err(e) => {
                warn!(%note_id, error = format!("{e:#}"), "skip: execute failed (native controller)");
                continue;
            }
        };
        let tx_id = result.executed_transaction().id();
        let prover = client.prover();
        let proven = match client.prove_transaction_with(&result, prover).await {
            Ok(p) => p,
            Err(e) => {
                warn!(%note_id, error = format!("{e:#}"), "skip: prove failed (native)");
                continue;
            }
        };
        let height = match client.submit_proven_transaction(proven, &result).await {
            Ok(h) => h,
            Err(e) => {
                warn!(%note_id, error = format!("{e:#}"), "skip: submit failed (native)");
                continue;
            }
        };
        if let Err(e) = client.apply_transaction(&result, height).await {
            warn!(%note_id, error = format!("{e:#}"), "apply warning (native)");
        }
        info!(
            %note_id, %tx_id, %height,
            "native deposit note consumed by controller (slot credited)",
        );
        consumed += 1;
    }
    info!(consumed, "native controller drain pass complete");
    Ok(())
}

async fn process_deposits(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    http: &reqwest::Client,
    relay_store_path: &str,
    relay_wallet: AccountId,
    controller: AccountId,
    deth_faucet: AccountId,
    note_script: &NoteScript,
) -> Result<()> {
    let pending = load_pending_intents(relay_store_path)?;
    if pending.is_empty() {
        info!("no pending intents — idle");
        return Ok(());
    }
    info!(count = pending.len(), "intents needing on-chain submission");

    // Pull live constituent prices once per pass — every pending intent
    // in this batch uses the same snapshot. The /api/prices endpoint is
    // already wired to CoinGecko via Vercel; the worker doesn't need its
    // own price feed. If the fetch fails we skip the whole pass rather
    // than mint into a wrong slot-10 credit, since the relay's mint
    // formula is (deposit_value * fee / nav_scale) and nav_scale is
    // derived from the basket NAV — a stale or zero NAV produces silently
    // huge mints (the bug we hit on 2026-06-06).
    let prices = match fetch_prices(http).await {
        Ok(p) => p,
        Err(e) => {
            warn!(
                error = format!("{e:#}"),
                "prices fetch failed — skipping pending intents this tick",
            );
            return Ok(());
        }
    };

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
        let amount_wei = match intent.amount_in_wei.parse::<u128>() {
            Ok(a) => a,
            Err(_) => {
                warn!(
                    correlation_id = %intent.correlation_id,
                    amount = %intent.amount_in_wei,
                    "amount doesn't parse — skipping",
                );
                mark_intent_error(
                    relay_store_path,
                    &intent.correlation_id,
                    "amount_in_wei unparseable",
                )?;
                continue;
            }
        };
        // The frontend sends amount_in_wei in EVM 18-decimal wei
        // (parseEther). The Miden dETH faucet operates in the 8-decimal
        // convention used across the Darwin Miden side (the Bali ETH
        // faucet is 8-dec too) and is test-grade — its max_supply is
        // only 1e7 base units, so a raw 1e13-wei amount can never be
        // backed. Map wei → 8-dec faucet base units (÷10^10): a
        // 0.00001 ETH deposit (1e13 wei) becomes 1000 dETH base units,
        // which fits both the faucet supply and the relay's holdings.
        let wei_per_miden_base = tunables().wei_per_miden_base;
        let amount: u64 = u64::try_from((amount_wei / wei_per_miden_base).max(1))
            .unwrap_or(u64::MAX);
        if vault_balance < amount {
            info!(
                correlation_id = %intent.correlation_id,
                need = amount,
                have = vault_balance,
                "vault under-funded — leaving for next tick",
            );
            continue;
        }

        let nav_usd = match basket_nav_usd(&intent.basket_symbol, &prices) {
            Some(n) if n > 0.0 => n,
            _ => {
                warn!(
                    correlation_id = %intent.correlation_id,
                    basket = %intent.basket_symbol,
                    "no NAV for basket symbol — skipping",
                );
                mark_intent_error(
                    relay_store_path,
                    &intent.correlation_id,
                    &format!("no NAV available for {}", intent.basket_symbol),
                )?;
                continue;
            }
        };

        match submit_atomic_deposit(
            client,
            relay_wallet,
            controller,
            deth_faucet,
            amount,
            prices.eth,
            nav_usd,
            note_script,
            &intent,
        )
        .await
        {
            Ok((tx_hex, note_hex, minted_basket_base)) => {
                info!(
                    correlation_id = %intent.correlation_id,
                    atomic_deposit_tx = %tx_hex,
                    note_id = %note_hex,
                    minted_basket_base,
                    "atomic_deposit_note submitted",
                );
                mark_intent_submitted(
                    relay_store_path,
                    &intent.correlation_id,
                    &tx_hex,
                    &note_hex,
                    minted_basket_base,
                )?;
                vault_balance -= amount;
            }
            Err(e) => {
                error!(
                    correlation_id = %intent.correlation_id,
                    error = format!("{e:#}"),
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

async fn process_redemptions(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    relay_store_path: &str,
    relay_wallet: AccountId,
    controller: AccountId,
    note_script: &NoteScript,
) -> Result<()> {
    let pending = load_pending_redemptions(relay_store_path)?;
    if pending.is_empty() {
        info!("no pending redemptions — idle");
        return Ok(());
    }
    info!(count = pending.len(), "redemptions needing on-chain submission");

    let relay_acct = client
        .get_account(relay_wallet)
        .await?
        .with_context(|| format!("relay wallet {} not in store", relay_wallet.to_hex()))?;

    for redemption in pending {
        let amount = match redemption.basket_amount.parse::<u64>() {
            Ok(a) => a,
            Err(_) => {
                warn!(
                    redemption_id = %redemption.redemption_id,
                    amount = %redemption.basket_amount,
                    "amount doesn't fit in u64 — skipping",
                );
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    "amount overflows u64",
                )?;
                continue;
            }
        };

        // The basket-token faucet is symbol-derived in a future iteration; for the M3
        // demo we discover it the same way flow_c_full does — pick a
        // fungible asset from the relay vault with balance >= amount.
        // The atomic redeem note is faucet-agnostic, the controller
        // accepts whatever vault key the note carries.
        let basket_faucet = match pick_basket_faucet(&relay_acct, amount) {
            Some(id) => id,
            None => {
                info!(
                    redemption_id = %redemption.redemption_id,
                    need = amount,
                    "no relay vault asset with sufficient balance — leaving for next tick",
                );
                continue;
            }
        };

        match submit_atomic_redeem(
            client,
            relay_wallet,
            controller,
            basket_faucet,
            amount,
            note_script,
            &redemption,
        )
        .await
        {
            Ok((tx_hex, note_hex)) => {
                info!(
                    redemption_id = %redemption.redemption_id,
                    miden_redeem_tx = %tx_hex,
                    note_id = %note_hex,
                    "atomic_redeem_note submitted",
                );
                mark_redemption_submitted(
                    relay_store_path,
                    &redemption.redemption_id,
                    &tx_hex,
                    &note_hex,
                )?;
            }
            Err(e) => {
                error!(
                    redemption_id = %redemption.redemption_id,
                    error = format!("{e:#}"),
                    "atomic_redeem submission failed",
                );
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("submission failed: {e}"),
                )?;
            }
        }
    }
    Ok(())
}

fn pick_basket_faucet(
    relay_acct: &miden_client::account::Account,
    min_amount: u64,
) -> Option<AccountId> {
    let mut candidates: Vec<(AccountId, u64)> = relay_acct
        .vault()
        .assets()
        .filter_map(|a| match a {
            Asset::Fungible(fa) => Some((fa.faucet_id(), fa.amount())),
            Asset::NonFungible(_) => None,
        })
        .filter(|(_, amt)| *amt >= min_amount)
        .collect();
    // Prefer the smallest sufficient balance so we don't drain a
    // large position to redeem a small one.
    candidates.sort_by_key(|(_, amt)| *amt);
    candidates.into_iter().next().map(|(id, _)| id)
}

async fn submit_atomic_deposit(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    relay_wallet: AccountId,
    controller: AccountId,
    deth_faucet: AccountId,
    amount: u64,
    asset_price_usd: f64,
    basket_nav_usd: f64,
    note_script: &NoteScript,
    intent: &PendingIntent,
) -> Result<(String, String, u64)> {
    let assets = NoteAssets::new(vec![Asset::Fungible(FungibleAsset::new(
        deth_faucet,
        amount,
    )?)])?;
    // Relay wallet is the note sender; the controller consumes it in
    // step 2 below (same shape as flow_a_full), which runs the note
    // script and credits slot-10.
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

    // Storage felts: atomic_deposit_note_v2 expects 7 felts and runs
    //   mint = deposit_value * fee_factor / nav_scale
    // under MASM `felt_div` (integer division). We pre-pack units so
    // the divide lands on the right basket-token base-unit count.
    //
    // Mirrors the frontend's `computeStorageFelts` in
    // src/components/MidenDepositPanel.tsx — must stay in sync.
    //
    //   deposit_value = amount_asset_base * asset_price_usd
    //   fee_factor    = 9970                  (0.9970 in 1e4 fp)
    //   nav_scale     = basket_nav_usd * 10000 / 10^(basket_dec - asset_dec)
    //
    // For dETH (8-dec) → DCC (8-dec) at NAV $25k, amount=1000 base
    // (0.00001 dETH @ $1547):
    //   deposit_value = 1000 * 1547  = 1_547_000
    //   nav_scale     = 25000 * 10000 = 250_000_000
    //   mint = 1_547_000 * 9970 / 250_000_000 ≈ 62 base units DCC
    //        = 0.00000062 DCC × $25000 ≈ $0.0155  ✓
    //
    // The prior bug used nav_scale = fee_factor (both 9970), making
    // them cancel and pinning the slot-10 credit to `amount` (1000
    // base units = $0.25-worth of DCC for a $0.016 deposit, an
    // ~16× over-mint).
    let fee_factor: u64 = 9_970;
    let asset_price_round = asset_price_usd.round().max(1.0) as u64;
    let deposit_value: u64 = amount.saturating_mul(asset_price_round).max(1);
    let nav_flat: u64 = basket_nav_usd.round().max(1.0) as u64;
    let basket_minus_asset_dec: i32 = BASKET_TOKEN_DECIMALS - ASSET_DECIMALS_DETH;
    let mut nav_scale: u64 = nav_flat.saturating_mul(10_000);
    if basket_minus_asset_dec > 0 {
        let div = 10u64.pow(basket_minus_asset_dec as u32);
        nav_scale = (nav_scale / div).max(1);
    } else if basket_minus_asset_dec < 0 {
        let mul = 10u64.pow((-basket_minus_asset_dec) as u32);
        nav_scale = nav_scale.saturating_mul(mul);
    }
    // Mint the controller will land in slot-10. Same formula the MASM
    // runs (deposit_value * fee_factor / nav_scale) — the worker
    // returns it so mark_intent_submitted can overwrite the HTTP
    // server's optimistic placeholder with the authoritative number.
    let expected_mint: u64 = ((deposit_value as u128)
        .saturating_mul(fee_factor as u128)
        / (nav_scale.max(1) as u128))
        .min(u64::MAX as u128) as u64;
    info!(
        correlation_id = %intent.correlation_id,
        amount,
        asset_price_usd,
        basket_nav_usd,
        deposit_value,
        fee_factor,
        nav_scale,
        expected_mint,
        "atomic_deposit nav math",
    );
    let mut storage_felts = vec![
        miden_client::Felt::new(deposit_value),
        miden_client::Felt::new(fee_factor),
        miden_client::Felt::new(nav_scale),
    ];
    if std::env::var("DARWIN_RELAY_V2_USE_V2_NOTE")
        .map(|v| v != "0")
        .unwrap_or(true)
    {
        // Encode the user's EVM address into two felts: lower 16 bytes
        // as suffix, upper 4 bytes (zero-padded) as prefix. Stable +
        // injective for 20-byte addresses.
        let evm_bytes = hex_to_evm(&intent.user_evm_addr)
            .unwrap_or([0u8; 20]);
        let mut suffix_buf = [0u8; 8];
        let mut prefix_buf = [0u8; 8];
        // suffix = last 8 bytes of EVM addr (LE u64)
        suffix_buf.copy_from_slice(&evm_bytes[12..20]);
        // prefix = next 8 bytes (bytes 4..12) of EVM addr
        prefix_buf.copy_from_slice(&evm_bytes[4..12]);
        let user_id_suffix = u64::from_le_bytes(suffix_buf);
        let user_id_prefix = u64::from_le_bytes(prefix_buf);
        // Field-element max is p - 1 < 2^64; mask top bit to ensure
        // we never overflow Felt's canonical range.
        let mask = (1u64 << 63) - 1;
        storage_felts.push(miden_client::Felt::new(user_id_suffix & mask));
        storage_felts.push(miden_client::Felt::new(user_id_prefix & mask));

        // basket_id felts (slots 5,6 in note storage). The script writes
        // slot-10 at key (user_suffix, user_prefix, basket_suffix,
        // basket_prefix); the portfolio frontend reads with the same
        // 4-felt key. Without these two felts the script reads them as
        // 0/0 and the deposit lands in a slot the frontend never queries.
        let basket_hex = basket_faucet_hex(&intent.basket_symbol).with_context(|| {
            format!(
                "unknown basket_symbol {:?} — add it to basket_faucet_hex()",
                intent.basket_symbol
            )
        })?;
        let basket_account = AccountId::from_hex(basket_hex)
            .with_context(|| format!("basket_faucet_hex for {} is not parseable", intent.basket_symbol))?;
        let basket_suffix_felt = basket_account.suffix();
        let basket_prefix_felt = basket_account.prefix().as_felt();
        info!(
            correlation_id = %intent.correlation_id,
            basket_symbol = %intent.basket_symbol,
            basket_suffix = format!("{basket_suffix_felt}"),
            basket_prefix = format!("{basket_prefix_felt}"),
            "atomic_deposit basket key felts",
        );
        storage_felts.push(basket_suffix_felt);
        storage_felts.push(basket_prefix_felt);
    }
    let recipient = NoteRecipient::new(
        serial_num,
        note_script.clone(),
        NoteStorage::new(storage_felts)?,
    );
    let note = Note::new(assets, metadata, recipient);
    let note_id = note.id();

    // Step 1: relay wallet emits the note carrying the dETH.
    let req = TransactionRequestBuilder::new()
        .own_output_notes(vec![note.clone()])
        .build()?;
    let result = client.execute_transaction(relay_wallet, req).await?;
    let tx_id = result.executed_transaction().id();
    let prover = client.prover();
    let proven = client.prove_transaction_with(&result, prover.clone()).await?;
    let height = client.submit_proven_transaction(proven, &result).await?;
    client.apply_transaction(&result, height).await?;
    info!(
        correlation_id = %intent.correlation_id,
        height = %height,
        emit_tx = %tx_id,
        "atomic_deposit note emitted (relay wallet)",
    );

    // Step 2: the controller consumes the note, running the note script
    // (receive_asset → controller vault, then accumulate slot-10) in the
    // controller's own context. This is the step that actually credits
    // the on-chain ledger — without it the note sits unconsumed and the
    // frontend's on-chain position never updates. Consumed unauthenticated
    // (the note object is passed directly), matching flow_a_full.
    //
    // NOTE: because write_user_position now ACCUMULATES, this consume
    // must run exactly once per note. The caller only reaches here for
    // intents with atomic_deposit_tx IS NULL, and persists the tx right
    // after, so a given intent is processed once.
    let consume_req = TransactionRequestBuilder::new()
        .input_notes(vec![(note.clone(), None)])
        .build()?;
    let consume_result = client
        .execute_transaction(controller, consume_req)
        .await
        .context("controller consume of atomic_deposit note")?;
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
    info!(
        correlation_id = %intent.correlation_id,
        consume_tx = %consume_tx_id,
        consume_height = %consume_height,
        "controller consumed atomic_deposit note (slot-10 credited)",
    );

    Ok((format!("{tx_id}"), format!("{note_id}"), expected_mint))
}

async fn submit_atomic_redeem(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    relay_wallet: AccountId,
    controller: AccountId,
    basket_faucet: AccountId,
    amount: u64,
    note_script: &NoteScript,
    redemption: &PendingRedemption,
) -> Result<(String, String)> {
    let assets = NoteAssets::new(vec![Asset::Fungible(FungibleAsset::new(
        basket_faucet,
        amount,
    )?)])?;
    // The note's `call.<receive_asset_root>` lands the basket-tokens
    // into the controller's vault. Controller is referenced only via
    // the burned MAST root inside the note script — sender metadata
    // stays the relay wallet so the basket-tokens originate from there.
    let _ = controller;
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

    // Storage felts per atomic_redeem_note.masm:
    //   [burn_amount, gross_release_factor, scale]
    // 9970/10000 = 99.7% net of the 30 bps redeem fee (env-tunable).
    let storage_felts = vec![
        miden_client::Felt::new(amount),
        miden_client::Felt::new(tunables().redeem_fee_net_bps),
        miden_client::Felt::new(1),
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
        redemption_id = %redemption.redemption_id,
        height = %height,
        "atomic_redeem tx confirmed",
    );
    Ok((format!("{tx_id}"), format!("{note_id}")))
}

// ---------------------------------------------------------------------------
// Outbound 1Click leg (Miden → Sepolia release for redemptions)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct OneClickQuoteReq<'a> {
    dry: bool,
    #[serde(rename = "depositMode")]
    deposit_mode: &'a str,
    #[serde(rename = "swapType")]
    swap_type: &'a str,
    #[serde(rename = "slippageTolerance")]
    slippage_tolerance: f64,
    #[serde(rename = "originAsset")]
    origin_asset: &'a str,
    #[serde(rename = "depositType")]
    deposit_type: &'a str,
    #[serde(rename = "destinationAsset")]
    destination_asset: &'a str,
    amount: String,
    #[serde(rename = "refundTo")]
    refund_to: String,
    #[serde(rename = "refundType")]
    refund_type: &'a str,
    recipient: String,
    #[serde(rename = "recipientType")]
    recipient_type: &'a str,
    deadline: &'a str,
}

#[derive(Deserialize, Debug)]
struct OneClickQuoteResp {
    #[serde(rename = "correlationId")]
    correlation_id: String,
    quote: OneClickQuote,
}

#[derive(Deserialize, Debug)]
struct OneClickQuote {
    #[serde(rename = "depositAddress")]
    deposit_address: String,
    #[serde(rename = "depositMemo", default)]
    deposit_memo: Option<String>,
    #[serde(rename = "amountOut")]
    #[allow(dead_code)]
    amount_out: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct BridgeOutMemo {
    #[serde(rename = "bridgeAccountId")]
    bridge_account_id: String,
    storage: BridgeOutMemoStorage,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct BridgeOutMemoStorage {
    storage_items: Vec<String>,
}

#[derive(Serialize)]
struct OneClickDepositSubmitReq<'a> {
    #[serde(rename = "txHash")]
    tx_hash: &'a str,
    #[serde(rename = "depositAddress")]
    deposit_address: &'a str,
}

// /demo/flows/outbound/submit body — the mock's outbound counterpart to
// /v0/deposit/submit. Used by process_outbound (bridge_out_v1) after
// the relay emits its P2ID; the mock takes senderAccountId (= the
// bridge_account where the P2ID was sent), polls for + consumes the
// note, then triggers its bridge solver for the Sepolia release.
#[derive(Serialize)]
struct DemoOutboundSubmitReq<'a> {
    #[serde(rename = "senderAccountId")]
    sender_account_id: &'a str,
    asset: &'a str,
    amount: String,
    recipient: &'a str,
    #[serde(rename = "refundTo")]
    refund_to: &'a str,
    #[serde(rename = "timeoutSecs")]
    timeout_secs: u64,
}

// Response from /demo/flows/outbound/submit. The mock runs the full
// outbound pipeline synchronously inside the wait window:
// consume_note → settlement_initiated → settlement_succeeded → evm_release.
// If timeoutSecs is large enough the response already carries the
// final flow.status and artifacts.evmReleaseTxHashes — no need to
// poll afterwards. Only the fields we use are typed; the rest are
// captured opaquely under `_other` so deserialization stays forgiving
// across mock version bumps.
#[derive(Deserialize, Debug)]
struct DemoOutboundSubmitResp {
    flow: DemoFlowResp,
    #[serde(rename = "bridgeOutNoteTxId")]
    #[allow(dead_code)]
    bridge_out_note_tx_id: Option<String>,
}

#[derive(Deserialize, Debug)]
struct DemoFlowResp {
    #[serde(rename = "correlationId")]
    correlation_id: String,
    status: String,
    artifacts: DemoFlowArtifacts,
}

#[derive(Deserialize, Debug, Default)]
struct DemoFlowArtifacts {
    #[serde(rename = "evmReleaseTxHashes", default)]
    evm_release_tx_hashes: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct OneClickStatusResp {
    status: String,
    #[serde(rename = "swapDetails", default)]
    swap_details: Option<OneClickSwapDetails>,
}

#[derive(Deserialize, Debug, Default)]
struct OneClickSwapDetails {
    #[serde(rename = "destinationChainTxHashes", default)]
    destination_chain_tx_hashes: Vec<OneClickTxRef>,
}

#[derive(Deserialize, Debug)]
struct OneClickTxRef {
    hash: String,
}

#[allow(clippy::too_many_arguments)]
async fn process_outbound(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    relay_store_path: &str,
    relay_wallet: AccountId,
    oneclick_faucet: AccountId,
    oneclick_url: &str,
    http: &reqwest::Client,
    bridge_out_script: &NoteScript,
) -> Result<()> {
    let pending = load_outbound_pending(relay_store_path)?;
    if pending.is_empty() {
        info!("no outbound legs pending — idle");
        return Ok(());
    }
    info!(count = pending.len(), "redemptions needing outbound bridge");

    let relay_acct = client
        .get_account(relay_wallet)
        .await?
        .with_context(|| format!("relay wallet {} not in store", relay_wallet.to_hex()))?;
    let vault_balance = relay_acct.vault().get_balance(oneclick_faucet).unwrap_or(0);
    info!(
        relay_wallet = %relay_wallet.to_hex(),
        oneclick_faucet = %oneclick_faucet.to_hex(),
        balance = vault_balance,
        "relay outbound vault snapshot",
    );

    for redemption in pending {
        let basket_amount: u64 = match redemption.basket_amount.parse() {
            Ok(a) => a,
            Err(_) => {
                warn!(
                    redemption_id = %redemption.redemption_id,
                    amount = %redemption.basket_amount,
                    "amount doesn't fit in u64 — skipping",
                );
                continue;
            }
        };
        // 99.7% net of 30 bps redeem fee, matching the on-chain note's
        // gross_release_factor. a future iteration routes through Pragma + pro-rata.
        let underlying = basket_amount.saturating_mul(tunables().redeem_fee_net_bps) / 10_000;

        if vault_balance < underlying {
            info!(
                redemption_id = %redemption.redemption_id,
                need = underlying,
                have = vault_balance,
                "outbound faucet under-funded — leaving for next tick",
            );
            continue;
        }

        // 1) Quote 1Click for Miden -> Sepolia.
        let quote_req = OneClickQuoteReq {
            dry: false,
            deposit_mode: "SIMPLE",
            swap_type: "EXACT_INPUT",
            slippage_tolerance: 100.0,
            origin_asset: ONECLICK_ORIGIN_ASSET,
            deposit_type: "ORIGIN_CHAIN",
            destination_asset: ONECLICK_DEST_ASSET,
            amount: underlying.to_string(),
            refund_to: relay_wallet.to_hex(),
            refund_type: "ORIGIN_CHAIN",
            recipient: redemption.user_evm_addr.clone(),
            recipient_type: "DESTINATION_CHAIN",
            deadline: "2027-01-01T00:00:00Z",
        };
        // /v0/quote also flakes on transient bridge mock conditions
        // (container restart mid-call, sqlx pool blip). Without retry,
        // a quote failure here error-marks the redemption AND
        // auto-recovers (re-credits the position off-chain) — but the
        // atomic_redeem burn has already happened on-chain, so the
        // basket-token supply diverges from the off-chain ledger by
        // the burn amount. Same retry shape as the demo outbound
        // submit below: 4xx is permanent, 5xx + network are retried
        // with 2s/5s/10s backoff (~17s total budget). When the budget
        // exhausts, fall through to the existing error path (sunk
        // burn is the cost of a stranded mock).
        let url = format!("{}/v0/quote", oneclick_url.trim_end_matches('/'));
        const QUOTE_RETRY_DELAYS_SECS: [u64; 3] = [2, 5, 10];
        let mut quote_resp: Option<reqwest::Response> = None;
        let mut quote_err_summary: Option<String> = None;
        for attempt in 0u8..4 {
            match http.post(&url).json(&quote_req).send().await {
                Ok(r) if r.status().is_success() => {
                    quote_resp = Some(r);
                    if attempt > 0 {
                        info!(
                            redemption_id = %redemption.redemption_id,
                            attempt,
                            "1Click quote recovered after retry",
                        );
                    }
                    break;
                }
                Ok(r) => {
                    let code = r.status();
                    let body = r.text().await.unwrap_or_default();
                    let body_truncated: String = body.chars().take(200).collect();
                    if !code.is_server_error() {
                        quote_err_summary =
                            Some(format!("{code} (permanent, no retry): {body_truncated}"));
                        break;
                    }
                    if attempt as usize >= QUOTE_RETRY_DELAYS_SECS.len() {
                        quote_err_summary =
                            Some(format!("{code}: {body_truncated} (retries exhausted)"));
                        break;
                    }
                    let backoff = QUOTE_RETRY_DELAYS_SECS[attempt as usize];
                    warn!(
                        redemption_id = %redemption.redemption_id,
                        attempt,
                        %code,
                        backoff_secs = backoff,
                        "1Click quote 5xx — backing off and retrying",
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                }
                Err(e) => {
                    if attempt as usize >= QUOTE_RETRY_DELAYS_SECS.len() {
                        quote_err_summary = Some(format!(
                            "network: {e:#} (retries exhausted)"
                        ));
                        break;
                    }
                    let backoff = QUOTE_RETRY_DELAYS_SECS[attempt as usize];
                    warn!(
                        redemption_id = %redemption.redemption_id,
                        attempt,
                        error = format!("{e:#}"),
                        backoff_secs = backoff,
                        "1Click quote HTTP error — backing off and retrying",
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                }
            }
        }
        let resp = match quote_resp {
            Some(r) => r,
            None => {
                let err = quote_err_summary
                    .unwrap_or_else(|| "1Click quote failed (no diagnostic)".to_string());
                error!(redemption_id = %redemption.redemption_id, %err, "1Click quote http failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("1Click quote http: {err}"),
                )?;
                continue;
            }
        };
        let quote: OneClickQuoteResp = match resp.json().await {
            Ok(q) => q,
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "1Click quote decode failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("1Click quote decode: {e}"),
                )?;
                continue;
            }
        };
        // The outbound P2ID is NOT a standard P2ID — Brian's bridge
        // expects a bridge_out_v1 note carrying the quote's storage
        // items so the bridge listener can correlate the inbound note
        // with the original quote. Extract the encoded memo + storage
        // items from the quote response.
        let memo_str = match quote.quote.deposit_memo.as_deref() {
            Some(s) if !s.is_empty() => s,
            _ => {
                error!(redemption_id = %redemption.redemption_id, "quote has no depositMemo");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    "1Click quote missing depositMemo",
                )?;
                continue;
            }
        };
        let memo: BridgeOutMemo = match serde_json::from_str(memo_str) {
            Ok(m) => m,
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "depositMemo decode failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("depositMemo decode: {e}"),
                )?;
                continue;
            }
        };
        let bridge_account = match AccountId::from_hex(&memo.bridge_account_id) {
            Ok(id) => id,
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = ?e, "bridge_account_id parse failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("bridge_account_id parse: {e:?}"),
                )?;
                continue;
            }
        };
        let storage_felts: Vec<miden_client::Felt> = match memo
            .storage
            .storage_items
            .iter()
            .map(|s| s.parse::<u64>().map(miden_client::Felt::new))
            .collect::<std::result::Result<_, _>>()
        {
            Ok(v) => v,
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "storage_items parse failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("storage_items parse: {e}"),
                )?;
                continue;
            }
        };
        info!(
            redemption_id = %redemption.redemption_id,
            correlation_id = %quote.correlation_id,
            bridge_account = %memo.bridge_account_id,
            storage_items = storage_felts.len(),
            "1Click outbound quote (bridge_out_v1)",
        );

        // 2) Build the bridge_out_v1 note: same script + storage Brian
        // mints on the quote side, NoteTag::with_account_target so the
        // bridge listener picks it up.
        let tag = NoteTag::with_account_target(bridge_account);
        let metadata = NoteMetadata::new(relay_wallet, NoteType::Public)
            .with_tag(tag)
            .with_attachment(NoteAttachment::default());
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
        let storage = NoteStorage::new(storage_felts)?;
        let recipient = NoteRecipient::new(serial_num, bridge_out_script.clone(), storage);
        let assets = NoteAssets::new(vec![Asset::Fungible(FungibleAsset::new(
            oneclick_faucet,
            underlying,
        )?)])?;
        let bridge_note = Note::new(assets, metadata, recipient);
        let req = match TransactionRequestBuilder::new()
            .own_output_notes(vec![bridge_note])
            .build()
        {
            Ok(r) => r,
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "bridge-out build failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("bridge-out build: {e}"),
                )?;
                continue;
            }
        };
        let tx_hex = match client.execute_transaction(relay_wallet, req).await {
            Ok(result) => {
                let id = result.executed_transaction().id();
                let prover = client.prover();
                let proven = match client.prove_transaction_with(&result, prover).await {
                    Ok(p) => p,
                    Err(e) => {
                        error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "outbound prove failed");
                        mark_redemption_error(
                            relay_store_path,
                            &redemption.redemption_id,
                            &format!("prove: {e}"),
                        )?;
                        continue;
                    }
                };
                let height = match client.submit_proven_transaction(proven, &result).await {
                    Ok(h) => h,
                    Err(e) => {
                        error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "outbound submit failed");
                        mark_redemption_error(
                            relay_store_path,
                            &redemption.redemption_id,
                            &format!("submit: {e}"),
                        )?;
                        continue;
                    }
                };
                if let Err(e) = client.apply_transaction(&result, height).await {
                    warn!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "apply_transaction warning");
                }
                info!(
                    redemption_id = %redemption.redemption_id,
                    miden_bridge_out_tx = %id,
                    height = %height,
                    "outbound P2ID emitted",
                );
                format!("{id}")
            }
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "outbound execute failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("execute: {e}"),
                )?;
                continue;
            }
        };

        // 3) Trigger the mock's outbound demo flow. The public
        // /v0/deposit/submit handler is INBOUND-only — its
        // get_quote_by_deposit() lookup is keyed on Sepolia deposit
        // addresses, which we don't have for outbound. The mock's
        // outbound rail sits under /demo/flows/outbound/submit; it
        // takes the bridge_account (where we just shipped dETH on
        // Miden) as senderAccountId, polls miden-client for notes
        // targeting that account, consumes the P2ID we emitted, then
        // hands off to the bridge solver for the Sepolia release.
        //
        // timeoutSecs=60 so wait_and_consume_notes returns within a
        // reasonable tick window — the relay's P2ID is already in the
        // mempool, so notes should be visible within a few seconds.
        // Errors here are warn-only: the redemption is already debited
        // off-chain and burned on-chain, so we don't roll back; the
        // status poller picks up settlement in the next tick.
        let submit_url = format!(
            "{}/demo/flows/outbound/submit",
            oneclick_url.trim_end_matches('/')
        );
        let relay_hex = relay_wallet.to_hex();
        let submit_body = DemoOutboundSubmitReq {
            sender_account_id: &memo.bridge_account_id,
            asset: "eth",
            amount: underlying.to_string(),
            recipient: &redemption.user_evm_addr,
            refund_to: &relay_hex,
            timeout_secs: 60,
        };
        // Retry on 5xx + network errors. The mock returns 500 on
        // transient conditions that resolve on their own — sqlx pool
        // exhaustion under load, miden-client RPC blips inside the
        // mock's wait_and_consume_notes, race conditions where the
        // mock starts polling before our P2ID propagates ("note
        // relevance check failed"). Without retry, a single transient
        // 500 strands the redemption at SETTLED forever: the dETH
        // ship-out is already on Miden but no Sepolia release ever
        // follows. 4xx is permanent (validation, bad request), don't
        // retry.
        //
        // Backoff 2s, 5s, 10s = 17s worst-case extra wait. The mock's
        // typical recovery on pool/RPC blip is sub-second; "note
        // relevance" takes a few seconds for the P2ID to land in the
        // mock's view. 3 retries covers all three failure classes.
        const RETRY_DELAYS_SECS: [u64; 3] = [2, 5, 10];
        let mut final_resp: Option<reqwest::Response> = None;
        let mut final_err_summary: Option<String> = None;
        for attempt in 0u8..4 {
            match http.post(&submit_url).json(&submit_body).send().await {
                Ok(r) if r.status().is_success() => {
                    final_resp = Some(r);
                    if attempt > 0 {
                        info!(
                            redemption_id = %redemption.redemption_id,
                            attempt,
                            "demo outbound submit recovered after retry",
                        );
                    }
                    break;
                }
                Ok(r) => {
                    let code = r.status();
                    let body = r.text().await.unwrap_or_default();
                    let body_truncated: String = body.chars().take(200).collect();
                    if !code.is_server_error() {
                        // 4xx is permanent — body shape mismatch, bad
                        // sender_account_id, etc. No point retrying.
                        final_err_summary = Some(format!(
                            "{code} (permanent, no retry): {body_truncated}"
                        ));
                        break;
                    }
                    let summary = format!("{code}: {body_truncated}");
                    if attempt as usize >= RETRY_DELAYS_SECS.len() {
                        final_err_summary = Some(format!("{summary} (retries exhausted)"));
                        break;
                    }
                    let backoff = RETRY_DELAYS_SECS[attempt as usize];
                    warn!(
                        redemption_id = %redemption.redemption_id,
                        attempt,
                        %code,
                        backoff_secs = backoff,
                        "demo outbound submit 5xx — backing off and retrying",
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                }
                Err(e) => {
                    let summary = format!("network: {e:#}");
                    if attempt as usize >= RETRY_DELAYS_SECS.len() {
                        final_err_summary = Some(format!("{summary} (retries exhausted)"));
                        break;
                    }
                    let backoff = RETRY_DELAYS_SECS[attempt as usize];
                    warn!(
                        redemption_id = %redemption.redemption_id,
                        attempt,
                        error = format!("{e:#}"),
                        backoff_secs = backoff,
                        "demo outbound submit HTTP error — backing off and retrying",
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                }
            }
        }
        // Map back into the original match shape: Ok(Response) on success,
        // synthetic Err on all-attempts-failed so the existing warn path
        // still fires with a useful error summary.
        let attempt_result: Result<reqwest::Response, std::io::Error> = match final_resp {
            Some(r) => Ok(r),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                final_err_summary
                    .unwrap_or_else(|| "demo outbound submit failed (no diagnostic)".to_string()),
            )),
        };
        match attempt_result {
            Ok(r) if r.status().is_success() => {
                match r.json::<DemoOutboundSubmitResp>().await {
                    Ok(resp) => {
                        info!(
                            redemption_id = %redemption.redemption_id,
                            demo_correlation_id = %resp.flow.correlation_id,
                            flow_status = %resp.flow.status,
                            release_tx_count = resp.flow.artifacts.evm_release_tx_hashes.len(),
                            "demo outbound submitted",
                        );
                        // The mock's submit_outbound returns after
                        // consuming the Miden note (~10s) — its bridge
                        // solver releases on Sepolia ~15-30s later.
                        // Poll /demo/flows/<id> a few times to catch
                        // the SUCCESS while we're already mid-flight;
                        // gives a clean atomic E2E test instead of
                        // depending on the next-tick poller (which
                        // would also work but spreads the visible
                        // outcome across two cycles).
                        let mut release_tx: Option<String> = if resp.flow.status == "SUCCESS" {
                            resp.flow.artifacts.evm_release_tx_hashes.first().cloned()
                        } else {
                            None
                        };
                        if release_tx.is_none() {
                            let flow_url = format!(
                                "{}/demo/flows/{}",
                                oneclick_url.trim_end_matches('/'),
                                resp.flow.correlation_id,
                            );
                            for _ in 0..15 {
                                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                                match http.get(&flow_url).send().await {
                                    Ok(fr) if fr.status().is_success() => {
                                        if let Ok(flow) = fr.json::<DemoFlowResp>().await {
                                            if flow.status == "SUCCESS" {
                                                release_tx = flow
                                                    .artifacts
                                                    .evm_release_tx_hashes
                                                    .first()
                                                    .cloned();
                                                break;
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        if let Some(release_tx) = release_tx {
                            if let Err(e) = mark_redemption_settled(
                                relay_store_path,
                                &redemption.redemption_id,
                                &release_tx,
                            ) {
                                warn!(
                                    redemption_id = %redemption.redemption_id,
                                    error = format!("{e:#}"),
                                    "mark_redemption_settled failed (will retry on next tick)",
                                );
                            } else {
                                info!(
                                    redemption_id = %redemption.redemption_id,
                                    sepolia_release_tx = %release_tx,
                                    "outbound settled on Sepolia",
                                );
                            }
                        } else {
                            warn!(
                                redemption_id = %redemption.redemption_id,
                                demo_correlation_id = %resp.flow.correlation_id,
                                "outbound polled out without SUCCESS — \
                                 mark_redemption_settled deferred to next tick poller",
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            redemption_id = %redemption.redemption_id,
                            error = format!("{e:#}"),
                            "demo outbound submitted but response decode failed",
                        );
                    }
                }
            }
            Ok(r) => {
                let code = r.status();
                let body = r.text().await.unwrap_or_default();
                warn!(
                    redemption_id = %redemption.redemption_id,
                    %code,
                    body = %body.chars().take(200).collect::<String>(),
                    "demo outbound submit non-2xx (continuing)"
                );
            }
            Err(e) => {
                warn!(
                    redemption_id = %redemption.redemption_id,
                    error = format!("{e:#}"),
                    "demo outbound submit http failed (continuing)"
                );
            }
        }

        // 4) Persist. Legacy 1Click path doesn't go through the
        // canonical Bali bridge, so no baseline_cnt is meaningful.
        mark_redemption_outbound(
            relay_store_path,
            &redemption.redemption_id,
            &tx_hex,
            &quote.correlation_id,
            &quote.quote.deposit_address,
            None,
        )?;
    }
    Ok(())
}

async fn process_outbound_status(
    relay_store_path: &str,
    oneclick_url: &str,
    http: &reqwest::Client,
) -> Result<()> {
    let pending = load_outbound_in_flight(relay_store_path)?;
    if pending.is_empty() {
        info!("no outbound legs in flight — idle");
        return Ok(());
    }
    info!(count = pending.len(), "outbound legs in flight, polling 1Click");

    for (rid, deposit_address) in pending {
        let url = format!(
            "{}/v0/status?depositAddress={}",
            oneclick_url.trim_end_matches('/'),
            urlencode(&deposit_address),
        );
        let resp = match http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(rid = %rid, error = format!("{e:#}"), "1Click status http failed (continuing)");
                continue;
            }
        };
        if !resp.status().is_success() {
            warn!(rid = %rid, code = %resp.status(), "1Click status non-2xx");
            continue;
        }
        let status: OneClickStatusResp = match resp.json().await {
            Ok(s) => s,
            Err(e) => {
                warn!(rid = %rid, error = format!("{e:#}"), "1Click status decode failed");
                continue;
            }
        };
        info!(rid = %rid, status = %status.status, "1Click status");
        match status.status.as_str() {
            "SUCCESS" => {
                if let Some(details) = status.swap_details {
                    if let Some(release) = details.destination_chain_tx_hashes.first() {
                        mark_redemption_settled(
                            relay_store_path,
                            &rid,
                            &release.hash,
                        )?;
                        info!(rid = %rid, sepolia_release_tx = %release.hash, "outbound settled on Sepolia");
                    } else {
                        warn!(rid = %rid, "SUCCESS but no destinationChainTxHashes yet");
                    }
                }
            }
            "REFUNDED" | "FAILED" => {
                mark_redemption_error(
                    relay_store_path,
                    &rid,
                    &format!("1Click reported {}", status.status),
                )?;
            }
            _ => { /* still PENDING/PROCESSING — leave for next tick */ }
        }
    }
    Ok(())
}

fn hex_to_evm(s: &str) -> Option<[u8; 20]> {
    let h = s.strip_prefix("0x").unwrap_or(s);
    if h.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    for i in 0..20 {
        out[i] = u8::from_str_radix(&h[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

fn urlencode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            _ => format!("%{:02X}", c as u32),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// SQLite glue against the v2 service's store.
// ---------------------------------------------------------------------------

struct PendingIntent {
    correlation_id: String,
    user_evm_addr: String,
    amount_in_wei: String,
    basket_symbol: String,
}

struct PendingRedemption {
    redemption_id: String,
    basket_amount: String,
}

fn load_pending_intents(store_path: &str) -> Result<Vec<PendingIntent>> {
    let conn = Connection::open(store_path)?;
    let mut stmt = conn.prepare(
        r#"SELECT correlation_id, user_evm_addr, amount_in_wei, basket_symbol
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
            amount_in_wei: r.get(2)?,
            basket_symbol: r.get(3)?,
        });
    }
    Ok(out)
}

fn mark_intent_submitted(
    store_path: &str,
    correlation_id: &str,
    tx_hex: &str,
    note_hex: &str,
    minted_basket_base: u64,
) -> Result<()> {
    let conn = Connection::open(store_path)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    // Overwrite the optimistic basket_amount_minted the HTTP server
    // wrote on 1Click=SUCCESS (it estimated amount_in_wei ÷ 10^10,
    // which conflates asset base units with basket base units — the
    // same NAV-collapse bug we fixed in submit_atomic_deposit). The
    // worker's number is authoritative because it matches what the
    // controller's slot-10 actually credited (deposit_value *
    // fee_factor / nav_scale).
    conn.execute(
        r#"UPDATE intents
              SET atomic_deposit_tx    = ?2,
                  miden_consume_tx     = ?3,
                  basket_amount_minted = ?4,
                  updated_at           = ?5
              WHERE correlation_id = ?1"#,
        params![
            correlation_id,
            tx_hex,
            note_hex,
            minted_basket_base.to_string(),
            now,
        ],
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

fn load_pending_redemptions(store_path: &str) -> Result<Vec<PendingRedemption>> {
    let conn = Connection::open(store_path)?;
    let mut stmt = conn.prepare(
        r#"SELECT redemption_id, basket_amount
              FROM redemptions
              WHERE miden_redeem_tx IS NULL
                AND (error IS NULL OR error = '')
              ORDER BY created_at ASC"#,
    )?;
    let mut rows = stmt.query(params![])?;
    let mut out = vec![];
    while let Some(r) = rows.next()? {
        out.push(PendingRedemption {
            redemption_id: r.get(0)?,
            basket_amount: r.get(1)?,
        });
    }
    Ok(out)
}

fn mark_redemption_submitted(
    store_path: &str,
    redemption_id: &str,
    tx_hex: &str,
    note_hex: &str,
) -> Result<()> {
    let conn = Connection::open(store_path)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    // miden_redeem_tx = the burn-leg tx hash. miden_bridge_out_tx is
    // left null on purpose — it gets set by the outbound 1Click worker
    // once the controller has released underlyings back to the relay
    // vault and the bridge-out note has been emitted (future work).
    let _ = note_hex;
    conn.execute(
        r#"UPDATE redemptions
              SET miden_redeem_tx = ?2,
                  updated_at      = ?3
              WHERE redemption_id = ?1"#,
        params![redemption_id, tx_hex, now],
    )?;
    Ok(())
}

fn mark_redemption_error(
    store_path: &str,
    redemption_id: &str,
    msg: &str,
) -> Result<()> {
    let conn = Connection::open(store_path)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;

    // Auto-recovery: the REST handler debits the user's position
    // BEFORE the worker emits anything on-chain (the redemption stage
    // is flipped to SETTLED immediately so the UI reflects intent).
    // If the worker then permanently fails to emit, the off-chain
    // debit would otherwise be unrecoverable. Re-credit the position
    // on the FIRST error mark so the user is whole.
    //
    // Idempotency: only re-credit when the existing error column is
    // NULL. A second call to mark_redemption_error (later retries by
    // the worker after the error was already recorded) only updates
    // the error message.
    let row = conn
        .query_row(
            r#"SELECT error IS NULL AS first_time,
                      user_evm_addr,
                      basket_symbol,
                      basket_amount
                  FROM redemptions
                  WHERE redemption_id = ?1"#,
            params![redemption_id],
            |r| {
                Ok((
                    r.get::<_, bool>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            },
        )
        .ok();

    if let Some((first_time, user, symbol, amount)) = row {
        if first_time {
            // Same upsert shape REST's credit_position uses — adds the
            // amount back, stamps a recovery marker as the
            // last_correlation_id so an operator can trace the credit
            // back to the originating redemption.
            conn.execute(
                r#"INSERT INTO positions
                      (user_evm_addr, basket_symbol, basket_amount,
                       last_correlation_id, last_updated)
                    VALUES (?1, ?2, ?3, ?4, ?5)
                    ON CONFLICT(user_evm_addr, basket_symbol) DO UPDATE SET
                       basket_amount       = CAST(CAST(basket_amount AS INTEGER)
                                                 + CAST(excluded.basket_amount AS INTEGER) AS TEXT),
                       last_correlation_id = excluded.last_correlation_id,
                       last_updated        = excluded.last_updated"#,
                params![
                    user,
                    symbol,
                    amount,
                    format!("recovery-of-{}", redemption_id),
                    now,
                ],
            )?;
            info!(
                redemption_id = %redemption_id,
                user = %user,
                basket = %symbol,
                amount = %amount,
                "auto-recovered: redemption errored → position re-credited",
            );
        }
    }

    conn.execute(
        r#"UPDATE redemptions
              SET error      = ?2,
                  updated_at = ?3
              WHERE redemption_id = ?1"#,
        params![redemption_id, msg, now],
    )?;
    Ok(())
}

struct OutboundPending {
    redemption_id: String,
    user_evm_addr: String,
    basket_amount: String,
}

/// Canonical Bali outbound: emit a B2AGG note from the relay wallet
/// → user's Sepolia EOA via the Bali bridge account. Permissionless
/// (no 1Click solver in the loop). Settles on the L1 side once the
/// user (or anyone) calls `claimAsset` with the merkle proof —
/// `process_outbound_status_b2agg` watches the bridge service for
/// that and writes back to sqlite.
/// Snapshot the bridge service's view of `user_evm_addr` and return
/// the highest `deposit_cnt` it knows about — or None on network /
/// decode failure. Used to baseline a b2agg burn before emit: the
/// post-emit deposit will have a strictly higher cnt, which lets the
/// claim-watcher disambiguate it from historical same-amount entries.
async fn fetch_bali_baseline_cnt(
    http: &reqwest::Client,
    bridge_service_url: &str,
    user_evm_addr: &str,
) -> Option<u32> {
    let url = format!(
        "{}/api/bridges/{}",
        bridge_service_url.trim_end_matches('/'),
        user_evm_addr.to_lowercase(),
    );
    let resp = http.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: serde_json::Value = resp.json().await.ok()?;
    body.get("deposits")?
        .as_array()?
        .iter()
        .filter_map(|d| d.get("deposit_cnt").and_then(|v| v.as_u64()))
        .max()
        .map(|m| m as u32)
}

async fn process_outbound_b2agg(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    relay_store_path: &str,
    relay_wallet: AccountId,
    bali_bridge: AccountId,
    bali_faucet: AccountId,
    bridge_service_url: &str,
    http: &reqwest::Client,
) -> Result<()> {
    let pending = load_outbound_pending(relay_store_path)?;
    if pending.is_empty() {
        info!("no outbound legs pending — idle (b2agg)");
        return Ok(());
    }
    info!(count = pending.len(), "redemptions needing outbound bridge (b2agg)");

    let relay_acct = client
        .get_account(relay_wallet)
        .await?
        .with_context(|| format!("relay wallet {} not in store", relay_wallet.to_hex()))?;
    let vault_balance = relay_acct.vault().get_balance(bali_faucet).unwrap_or(0);
    info!(
        relay_wallet = %relay_wallet.to_hex(),
        bali_faucet  = %bali_faucet.to_hex(),
        balance      = vault_balance,
        "relay outbound vault snapshot (b2agg)",
    );

    for redemption in pending {
        let basket_amount: u64 = match redemption.basket_amount.parse() {
            Ok(a) => a,
            Err(_) => {
                warn!(
                    redemption_id = %redemption.redemption_id,
                    amount = %redemption.basket_amount,
                    "amount doesn't fit in u64 — skipping",
                );
                continue;
            }
        };
        // Same 30 bps redeem-fee net the legacy path applied. a future iteration will
        // route through Pragma + per-constituent pro-rata.
        let underlying = basket_amount.saturating_mul(tunables().redeem_fee_net_bps) / 10_000;

        if vault_balance < underlying {
            info!(
                redemption_id = %redemption.redemption_id,
                need = underlying,
                have = vault_balance,
                "outbound faucet under-funded — leaving for next tick",
            );
            continue;
        }

        let l1_dest = match EthAddress::from_hex(&redemption.user_evm_addr) {
            Ok(a) => a,
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = ?e, "user_evm_addr parse failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("user_evm_addr parse: {e:?}"),
                )?;
                continue;
            }
        };

        let asset = match FungibleAsset::new(bali_faucet, underlying) {
            Ok(a) => Asset::Fungible(a),
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "fungible asset build failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("fungible asset: {e}"),
                )?;
                continue;
            }
        };
        let assets = match NoteAssets::new(vec![asset]) {
            Ok(a) => a,
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "note assets build failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("note assets: {e}"),
                )?;
                continue;
            }
        };

        // Capture the bridge-service baseline BEFORE we emit the burn
        // — our resulting deposit_cnt is guaranteed to be strictly
        // greater than every cnt visible at this instant, so the
        // claim-watcher can later filter out historical same-amount
        // entries that would otherwise be falsely attributed.
        let baseline_cnt =
            fetch_bali_baseline_cnt(http, bridge_service_url, &redemption.user_evm_addr).await;

        let b2agg = match B2AggNote::create(
            0, // destination_network = Ethereum L1 (Sepolia)
            l1_dest,
            assets,
            bali_bridge,
            relay_wallet,
            client.rng(),
        ) {
            Ok(n) => n,
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "B2AGG note build failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("B2AGG build: {e}"),
                )?;
                continue;
            }
        };

        let req = match TransactionRequestBuilder::new()
            .own_output_notes(vec![b2agg])
            .build()
        {
            Ok(r) => r,
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "B2AGG tx-request build failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("B2AGG req: {e}"),
                )?;
                continue;
            }
        };

        let tx_hex = match client.execute_transaction(relay_wallet, req).await {
            Ok(result) => {
                let id = result.executed_transaction().id();
                let prover = client.prover();
                let proven = match client.prove_transaction_with(&result, prover).await {
                    Ok(p) => p,
                    Err(e) => {
                        error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "B2AGG prove failed");
                        mark_redemption_error(
                            relay_store_path,
                            &redemption.redemption_id,
                            &format!("B2AGG prove: {e}"),
                        )?;
                        continue;
                    }
                };
                let height = match client.submit_proven_transaction(proven, &result).await {
                    Ok(h) => h,
                    Err(e) => {
                        error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "B2AGG submit failed");
                        mark_redemption_error(
                            relay_store_path,
                            &redemption.redemption_id,
                            &format!("B2AGG submit: {e}"),
                        )?;
                        continue;
                    }
                };
                if let Err(e) = client.apply_transaction(&result, height).await {
                    warn!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "B2AGG apply_transaction warning");
                }
                info!(
                    redemption_id = %redemption.redemption_id,
                    miden_bridge_out_tx = %id,
                    height = %height,
                    "B2AGG outbound emitted",
                );
                format!("{id}")
            }
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = format!("{e:#}"), "B2AGG execute failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("B2AGG execute: {e}"),
                )?;
                continue;
            }
        };

        // For the b2agg path there's no 1Click correlation — write
        // the Miden tx hash and leave oneclick_correlation empty.
        // process_outbound_status_b2agg watches the bridge service
        // for the claim_tx_hash later.
        mark_redemption_outbound(
            relay_store_path,
            &redemption.redemption_id,
            &tx_hex,
            "", // no 1Click correlation_id
            "", // no 1Click deposit_address
            baseline_cnt,
        )?;
    }
    Ok(())
}

/// Drain the `pending_bridge_outs` queue: each row is a user-initiated
/// canonical Bali outbound from the relay vault (not tied to any
/// basket position), enqueued by the REST POST /v0/bridge-out handler.
/// Builds + submits a B2AggNote, writes back the Miden tx id, and
/// flips the row's status so the GET endpoint reflects the result.
///
/// Same B2AggNote::create call shape as `process_outbound_b2agg`; the
/// difference is the trigger (REST request) and that there's no
/// basket-burn step or 30 bps redeem-fee.
async fn process_direct_bridge_outs(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    relay_store_path: &str,
    relay_wallet: AccountId,
    bali_bridge: AccountId,
    bali_faucet: AccountId,
) -> Result<()> {
    let pending = load_pending_bridge_outs(relay_store_path)?;
    if pending.is_empty() {
        info!("no direct bridge-outs pending — idle");
        return Ok(());
    }
    info!(count = pending.len(), "direct bridge-outs to process");

    let relay_acct = client
        .get_account(relay_wallet)
        .await?
        .with_context(|| format!("relay wallet {} not in store", relay_wallet.to_hex()))?;
    let vault_balance = relay_acct.vault().get_balance(bali_faucet).unwrap_or(0);

    for row in pending {
        let amount: u64 = match row.amount.parse() {
            Ok(a) => a,
            Err(_) => {
                warn!(
                    request_id = %row.request_id,
                    amount = %row.amount,
                    "amount doesn't fit in u64 — failing the request",
                );
                mark_bridge_out_failed(
                    relay_store_path,
                    &row.request_id,
                    &format!("amount {} doesn't fit in u64", row.amount),
                )?;
                continue;
            }
        };

        if vault_balance < amount {
            // Same "leave for next tick" semantics as the redemption
            // path — under-funding is transient (someone may top the
            // vault up), so we don't mark failed.
            info!(
                request_id = %row.request_id,
                need = amount,
                have = vault_balance,
                "relay vault under-funded for direct bridge-out — leaving for next tick",
            );
            continue;
        }

        let l1_dest = match EthAddress::from_hex(&row.dest_address) {
            Ok(a) => a,
            Err(e) => {
                error!(request_id = %row.request_id, error = ?e, "dest_address parse failed");
                mark_bridge_out_failed(
                    relay_store_path,
                    &row.request_id,
                    &format!("dest_address parse: {e:?}"),
                )?;
                continue;
            }
        };

        let asset = match FungibleAsset::new(bali_faucet, amount) {
            Ok(a) => Asset::Fungible(a),
            Err(e) => {
                error!(request_id = %row.request_id, error = format!("{e:#}"), "fungible asset build failed");
                mark_bridge_out_failed(
                    relay_store_path,
                    &row.request_id,
                    &format!("fungible asset: {e}"),
                )?;
                continue;
            }
        };
        let assets = match NoteAssets::new(vec![asset]) {
            Ok(a) => a,
            Err(e) => {
                error!(request_id = %row.request_id, error = format!("{e:#}"), "note assets build failed");
                mark_bridge_out_failed(
                    relay_store_path,
                    &row.request_id,
                    &format!("note assets: {e}"),
                )?;
                continue;
            }
        };

        let b2agg = match B2AggNote::create(
            0, // destination_network = Ethereum L1 (Sepolia)
            l1_dest,
            assets,
            bali_bridge,
            relay_wallet,
            client.rng(),
        ) {
            Ok(n) => n,
            Err(e) => {
                error!(request_id = %row.request_id, error = format!("{e:#}"), "B2AGG note build failed");
                mark_bridge_out_failed(
                    relay_store_path,
                    &row.request_id,
                    &format!("B2AGG build: {e}"),
                )?;
                continue;
            }
        };

        let req = match TransactionRequestBuilder::new()
            .own_output_notes(vec![b2agg])
            .build()
        {
            Ok(r) => r,
            Err(e) => {
                error!(request_id = %row.request_id, error = format!("{e:#}"), "B2AGG tx-request build failed");
                mark_bridge_out_failed(
                    relay_store_path,
                    &row.request_id,
                    &format!("B2AGG req: {e}"),
                )?;
                continue;
            }
        };

        let tx_hex = match client.execute_transaction(relay_wallet, req).await {
            Ok(result) => {
                let id = result.executed_transaction().id();
                let prover = client.prover();
                let proven = match client.prove_transaction_with(&result, prover).await {
                    Ok(p) => p,
                    Err(e) => {
                        error!(request_id = %row.request_id, error = format!("{e:#}"), "B2AGG prove failed");
                        mark_bridge_out_failed(
                            relay_store_path,
                            &row.request_id,
                            &format!("B2AGG prove: {e}"),
                        )?;
                        continue;
                    }
                };
                let height = match client.submit_proven_transaction(proven, &result).await {
                    Ok(h) => h,
                    Err(e) => {
                        error!(request_id = %row.request_id, error = format!("{e:#}"), "B2AGG submit failed");
                        mark_bridge_out_failed(
                            relay_store_path,
                            &row.request_id,
                            &format!("B2AGG submit: {e}"),
                        )?;
                        continue;
                    }
                };
                if let Err(e) = client.apply_transaction(&result, height).await {
                    warn!(request_id = %row.request_id, error = format!("{e:#}"), "B2AGG apply_transaction warning");
                }
                info!(
                    request_id = %row.request_id,
                    miden_tx_id = %id,
                    height = %height,
                    "direct bridge-out B2AGG emitted",
                );
                format!("{id}")
            }
            Err(e) => {
                error!(request_id = %row.request_id, error = format!("{e:#}"), "B2AGG execute failed");
                mark_bridge_out_failed(
                    relay_store_path,
                    &row.request_id,
                    &format!("B2AGG execute: {e}"),
                )?;
                continue;
            }
        };

        mark_bridge_out_submitted(relay_store_path, &row.request_id, &tx_hex)?;
    }

    Ok(())
}

struct PendingBridgeOut {
    request_id: String,
    dest_address: String,
    amount: String,
}

fn load_pending_bridge_outs(store_path: &str) -> Result<Vec<PendingBridgeOut>> {
    let conn = Connection::open(store_path)?;
    let mut stmt = conn.prepare(
        r#"SELECT request_id, dest_address, amount
              FROM pending_bridge_outs
              WHERE status = 'pending'
              ORDER BY created_at ASC
              LIMIT 10"#,
    )?;
    let mut rows = stmt.query(params![])?;
    let mut out = vec![];
    while let Some(r) = rows.next()? {
        out.push(PendingBridgeOut {
            request_id: r.get(0)?,
            dest_address: r.get(1)?,
            amount: r.get(2)?,
        });
    }
    Ok(out)
}

fn mark_bridge_out_submitted(store_path: &str, request_id: &str, miden_tx_id: &str) -> Result<()> {
    let conn = Connection::open(store_path)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    conn.execute(
        r#"UPDATE pending_bridge_outs
              SET status      = 'submitted',
                  miden_tx_id = ?2,
                  error       = NULL,
                  updated_at  = ?3
              WHERE request_id = ?1"#,
        params![request_id, miden_tx_id, now],
    )?;
    Ok(())
}

fn mark_bridge_out_failed(store_path: &str, request_id: &str, err: &str) -> Result<()> {
    let conn = Connection::open(store_path)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    conn.execute(
        r#"UPDATE pending_bridge_outs
              SET status     = 'failed',
                  error      = ?2,
                  updated_at = ?3
              WHERE request_id = ?1"#,
        params![request_id, err, now],
    )?;
    Ok(())
}

/// Watch the Bali bridge service for L1 claim transactions matching
/// the redemptions we have in flight, and update sqlite when one
/// lands. This is the b2agg analogue of `process_outbound_status`
/// (which polls 1Click for completion). Without this step the
/// RelayRedemptionsPanel never shows a sepolia_release_tx for a
/// redemption that's actually been claimed on Sepolia.
async fn process_outbound_status_b2agg(
    relay_store_path: &str,
    bridge_service_url: &str,
    http: &reqwest::Client,
) -> Result<()> {
    let conn = Connection::open(relay_store_path)?;
    // Only poll b2agg-mode outbounds: oneclick_correlation_id IS NULL
    // means "canonical Bali" (no 1Click correlation was attached).
    // Legacy bridge_out_v1 (1Click mock) entries have it set. The
    // typed column is the post-split shape; old rows are backfilled
    // by the startup migration so the NULL check is consistent across
    // history.
    // AggLayer certs settle in 30-90 min, so polling Bali for
    // redemptions younger than ~25 min is just generating load —
    // the answer is guaranteed to be "not yet". Skip them; they'll
    // be picked up on a later tick.
    let earliest_settleable = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64
        - 1500; // 25 minutes
    let mut stmt = conn.prepare(
        r#"SELECT redemption_id, user_evm_addr, basket_amount, bali_baseline_cnt
              FROM redemptions
              WHERE miden_bridge_out_tx IS NOT NULL
                AND (sepolia_release_tx IS NULL OR sepolia_release_tx = '')
                AND (error IS NULL OR error = '')
                AND oneclick_correlation_id IS NULL
                AND created_at <= ?1
              ORDER BY created_at ASC"#,
    )?;
    let rows = stmt
        .query_map(params![earliest_settleable], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<u32>>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);
    drop(conn);

    if rows.is_empty() {
        return Ok(());
    }
    info!(count = rows.len(), "polling bali bridge service for L1 claims");

    for (redemption_id, user_evm_addr, basket_amount, baseline_cnt) in rows {
        let basket_amount_u64: u64 = match basket_amount.parse() {
            Ok(a) => a,
            Err(_) => continue,
        };
        // The relay's basket_amount is in Miden 8-decimal base units
        // (same scale as the Bali ETH faucet on the L2 side). The
        // agglayer bridge service reports the amount as L1 wei
        // (18-decimal). The 10^10 factor closes the decimal gap
        // between Miden's 8-dec faucet representation and Sepolia's
        // 18-dec ETH wei representation.
        let t = tunables();
        let expected_underlying_miden = basket_amount_u64.saturating_mul(t.redeem_fee_net_bps) / 10_000;
        let expected_underlying_wei = expected_underlying_miden.saturating_mul(t.wei_per_miden_base as u64);

        let url = format!(
            "{}/api/bridges/{}",
            bridge_service_url.trim_end_matches('/'),
            user_evm_addr.to_lowercase(),
        );
        let resp = match http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(redemption_id = %redemption_id, error = format!("{e:#}"), "bali bridge service http failed");
                continue;
            }
        };
        if !resp.status().is_success() {
            warn!(redemption_id = %redemption_id, status = %resp.status(), "bali bridge service non-2xx");
            continue;
        }
        let body: serde_json::Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!(redemption_id = %redemption_id, error = format!("{e:#}"), "bali bridge service decode failed");
                continue;
            }
        };
        let deposits = body.get("deposits").and_then(|d| d.as_array());
        let Some(deposits) = deposits else { continue };

        // Match by (deposit_cnt > baseline captured at burn-time) AND
        // (dest_addr eq user) AND (amount eq expected) AND
        // (claim_tx_hash present). The cnt > baseline guard prevents
        // historical claimed burns with the same amount (e.g. an old
        // 100 DCC redemption months ago) from being falsely attributed
        // to a new pending redemption that hasn't actually been
        // claim_tx'd yet. `baseline_cnt = None` means the burn was
        // emitted by code that pre-dates this disambiguation column —
        // fall back to the old (buggy) match so legacy rows still
        // eventually resolve.
        let cnt_floor = baseline_cnt.unwrap_or(0);
        for dep in deposits {
            let dest_addr = dep
                .get("dest_addr")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_lowercase();
            let amount = dep
                .get("amount")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let claim_tx = dep
                .get("claim_tx_hash")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let dest_net = dep.get("dest_net").and_then(|v| v.as_u64()).unwrap_or(99);
            let dep_cnt = dep
                .get("deposit_cnt")
                .and_then(|v| v.as_u64())
                .map(|v| v as u32)
                .unwrap_or(0);

            if dep_cnt <= cnt_floor {
                continue;
            }
            if dest_net != 0 {
                continue;
            }
            if dest_addr != user_evm_addr.to_lowercase() {
                continue;
            }
            if amount != expected_underlying_wei {
                continue;
            }
            if claim_tx.is_empty() {
                continue;
            }

            mark_redemption_sepolia_release(relay_store_path, &redemption_id, claim_tx)?;
            info!(
                redemption_id = %redemption_id,
                sepolia_release_tx = %claim_tx,
                "B2AGG claim landed on Sepolia, marked sepolia_release_tx",
            );
            break;
        }
    }

    Ok(())
}

/// Helper used by both the 1Click + b2agg status pollers — sets
/// `sepolia_release_tx` when the L1 release has been observed.
fn mark_redemption_sepolia_release(
    store_path: &str,
    redemption_id: &str,
    tx_hex: &str,
) -> Result<()> {
    let conn = Connection::open(store_path)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    conn.execute(
        r#"UPDATE redemptions
              SET sepolia_release_tx = ?2,
                  stage              = 'SETTLED',
                  updated_at         = ?3
              WHERE redemption_id = ?1"#,
        params![redemption_id, tx_hex, now],
    )?;
    Ok(())
}

fn load_outbound_pending(store_path: &str) -> Result<Vec<OutboundPending>> {
    let conn = Connection::open(store_path)?;
    let mut stmt = conn.prepare(
        r#"SELECT redemption_id, user_evm_addr, basket_amount
              FROM redemptions
              WHERE miden_redeem_tx     IS NOT NULL
                AND miden_bridge_out_tx IS NULL
                AND (error IS NULL OR error = '')
              ORDER BY created_at ASC"#,
    )?;
    let mut rows = stmt.query(params![])?;
    let mut out = vec![];
    while let Some(r) = rows.next()? {
        out.push(OutboundPending {
            redemption_id: r.get(0)?,
            user_evm_addr: r.get(1)?,
            basket_amount: r.get(2)?,
        });
    }
    Ok(out)
}

fn mark_redemption_outbound(
    store_path: &str,
    redemption_id: &str,
    tx_hex: &str,
    correlation_id: &str,
    deposit_address: &str,
    bali_baseline_cnt: Option<u32>,
) -> Result<()> {
    let conn = Connection::open(store_path)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    // Two columns now hold what the legacy `oneclick_correlation`
    // joint-by-pipe used to encode. The pipe-joined column is still
    // written for backward compat (older readers / dashboards), but
    // every new code path reads from the typed columns.
    let joined = format!("{correlation_id}|{deposit_address}");
    let cid = if correlation_id.is_empty() { None } else { Some(correlation_id) };
    let dep = if deposit_address.is_empty() { None } else { Some(deposit_address) };
    conn.execute(
        r#"UPDATE redemptions
              SET miden_bridge_out_tx        = ?2,
                  oneclick_correlation       = ?3,
                  oneclick_correlation_id    = ?4,
                  oneclick_deposit_address   = ?5,
                  bali_baseline_cnt          = ?6,
                  updated_at                 = ?7
              WHERE redemption_id = ?1"#,
        params![redemption_id, tx_hex, joined, cid, dep, bali_baseline_cnt, now],
    )?;
    Ok(())
}

fn load_outbound_in_flight(store_path: &str) -> Result<Vec<(String, String)>> {
    let conn = Connection::open(store_path)?;
    let mut stmt = conn.prepare(
        r#"SELECT redemption_id, oneclick_correlation
              FROM redemptions
              WHERE miden_bridge_out_tx IS NOT NULL
                AND sepolia_release_tx  IS NULL
                AND oneclick_correlation IS NOT NULL
                AND (error IS NULL OR error = '')
              ORDER BY created_at ASC"#,
    )?;
    let mut rows = stmt.query(params![])?;
    let mut out = vec![];
    while let Some(r) = rows.next()? {
        let id: String = r.get(0)?;
        let joined: String = r.get(1)?;
        if let Some((_corr, deposit)) = joined.split_once('|') {
            out.push((id, deposit.to_string()));
        }
    }
    Ok(out)
}

fn mark_redemption_settled(
    store_path: &str,
    redemption_id: &str,
    sepolia_release_tx: &str,
) -> Result<()> {
    let conn = Connection::open(store_path)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    conn.execute(
        r#"UPDATE redemptions
              SET sepolia_release_tx = ?2,
                  stage              = 'FULLY_SETTLED',
                  updated_at         = ?3
              WHERE redemption_id = ?1"#,
        params![redemption_id, sepolia_release_tx, now],
    )?;
    Ok(())
}
