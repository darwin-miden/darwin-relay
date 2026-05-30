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
const ATOMIC_REDEEM_NOTE_MASM: &str = include_str!("../../asm/atomic_redeem_note.masm");
const MATH_MASM: &str = include_str!("../../asm/lib/math.masm");
const BRIDGE_OUT_V1_MASM: &str = include_str!("../../asm/bridge_out_v1.masm");

const DEFAULT_RELAY_WALLET_HEX: &str = "0xed3cd5befa3207805f8529207cfc0d";

// Tunables read once from env at first access. Magic numbers that
// used to be scattered through the call sites (decimal scaling, fee
// factor, redeem-fee net basis points) live here so M4 fee changes
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
    let deth_faucet = AccountId::from_hex(&faucet_hex)?;
    let oneclick_faucet = AccountId::from_hex(&oneclick_faucet_hex)?;
    let bali_bridge = AccountId::from_hex(&bali_bridge_hex)?;
    let bali_faucet = AccountId::from_hex(&bali_faucet_hex)?;

    info!(
        %store_path,
        %miden_store,
        %relay_wallet_hex,
        %controller_hex,
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
                error!(error = %e, "tick failed");
                // First 200 chars of the error so /v0/worker-health can
                // surface it without a log scrape.
                let s = format!("{e:?}");
                if s.len() > 200 { format!("{}…", &s[..200]) } else { s }
            }
        };
        if let Err(e) = write_heartbeat(&store_path, "main", &status) {
            warn!(error = %e, "heartbeat write failed");
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

    // Inbound notes (1Click deliveries) sit in COMMITTED state until
    // someone runs a consume tx against them. Drain them first so the
    // relay vault reflects the latest inflows before the other passes
    // snapshot it.
    if let Err(e) = drain_inbound_notes(client, relay_wallet).await {
        warn!(error = %e, "drain_inbound_notes failed (continuing)");
    }

    process_deposits(
        client,
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
                warn!(error = %e, "skip: not a consumable Note");
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
        let req = match TransactionRequestBuilder::new().build_consume_notes(vec![note]) {
            Ok(r) => r,
            Err(e) => {
                warn!(%note_id, error = %e, "skip: build_consume_notes failed");
                continue;
            }
        };
        let result = match client.execute_transaction(relay_wallet, req).await {
            Ok(r) => r,
            Err(e) => {
                warn!(%note_id, error = %e, "skip: execute failed");
                continue;
            }
        };
        let tx_id = result.executed_transaction().id();
        let prover = client.prover();
        let proven = match client.prove_transaction_with(&result, prover).await {
            Ok(p) => p,
            Err(e) => {
                warn!(%note_id, error = %e, "skip: prove failed");
                continue;
            }
        };
        let height = match client.submit_proven_transaction(proven, &result).await {
            Ok(h) => h,
            Err(e) => {
                warn!(%note_id, error = %e, "skip: submit failed");
                continue;
            }
        };
        if let Err(e) = client.apply_transaction(&result, height).await {
            warn!(%note_id, error = %e, "apply warning");
        }
        info!(%note_id, %tx_id, %height, "inbound note consumed");
        consumed += 1;
    }
    info!(consumed, "drain pass complete");
    Ok(())
}

async fn process_deposits(
    client: &mut miden_client::Client<FilesystemKeyStore>,
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

        // The basket-token faucet is symbol-derived in M4; for the M3
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
                    error = %e,
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
    note_script: &NoteScript,
    intent: &PendingIntent,
) -> Result<(String, String)> {
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

    // Storage felts: v1 atomic_deposit_note expects
    //   [deposit_value, fee_factor, nav_scale]
    // The note computes the slot-10 credit as
    //   deposit_value * fee_factor / nav_scale.
    // We want the on-chain credit to equal the deposited base-unit
    // amount (so it matches the off-chain RelayPositionsPanel, which
    // credits the same ÷10^10-scaled amount). Setting nav_scale =
    // fee_factor makes the two cancel → credit = deposit_value =
    // amount. (The earlier nav_scale=amount made deposit_value and
    // nav_scale cancel instead, pinning the credit to a constant
    // 9970 regardless of deposit size — which is what diverged from
    // the off-chain ledger.) The 30 bps redeem fee is applied on the
    // redeem leg, not here.
    let fee_factor = tunables().fee_factor;
    let nav_scale = fee_factor;
    let mut storage_felts = vec![
        miden_client::Felt::new(amount),
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

    Ok((format!("{tx_id}"), format!("{note_id}")))
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
        // gross_release_factor. M4 routes through Pragma + pro-rata.
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
        let url = format!("{}/v0/quote", oneclick_url.trim_end_matches('/'));
        let resp = match http.post(&url).json(&quote_req).send().await {
            Ok(r) => r,
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = %e, "1Click quote http failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("1Click quote http: {e}"),
                )?;
                continue;
            }
        };
        if !resp.status().is_success() {
            let code = resp.status();
            let body = resp.text().await.unwrap_or_default();
            error!(redemption_id = %redemption.redemption_id, %code, %body, "1Click quote non-2xx");
            mark_redemption_error(
                relay_store_path,
                &redemption.redemption_id,
                &format!("1Click quote {code}: {}", body.chars().take(200).collect::<String>()),
            )?;
            continue;
        }
        let quote: OneClickQuoteResp = match resp.json().await {
            Ok(q) => q,
            Err(e) => {
                error!(redemption_id = %redemption.redemption_id, error = %e, "1Click quote decode failed");
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
                error!(redemption_id = %redemption.redemption_id, error = %e, "depositMemo decode failed");
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
                error!(redemption_id = %redemption.redemption_id, error = %e, "storage_items parse failed");
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
                error!(redemption_id = %redemption.redemption_id, error = %e, "bridge-out build failed");
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
                        error!(redemption_id = %redemption.redemption_id, error = %e, "outbound prove failed");
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
                        error!(redemption_id = %redemption.redemption_id, error = %e, "outbound submit failed");
                        mark_redemption_error(
                            relay_store_path,
                            &redemption.redemption_id,
                            &format!("submit: {e}"),
                        )?;
                        continue;
                    }
                };
                if let Err(e) = client.apply_transaction(&result, height).await {
                    warn!(redemption_id = %redemption.redemption_id, error = %e, "apply_transaction warning");
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
                error!(redemption_id = %redemption.redemption_id, error = %e, "outbound execute failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
                    &format!("execute: {e}"),
                )?;
                continue;
            }
        };

        // 3) Notify 1Click that the inbound (from its POV) is in flight.
        let submit_url = format!("{}/v0/deposit/submit", oneclick_url.trim_end_matches('/'));
        let submit_body = OneClickDepositSubmitReq {
            tx_hash: &tx_hex,
            deposit_address: &quote.quote.deposit_address,
        };
        match http.post(&submit_url).json(&submit_body).send().await {
            Ok(r) if r.status().is_success() => {
                info!(redemption_id = %redemption.redemption_id, "1Click notified of outbound");
            }
            Ok(r) => {
                let code = r.status();
                warn!(
                    redemption_id = %redemption.redemption_id,
                    %code,
                    "1Click deposit/submit non-2xx (continuing)"
                );
            }
            Err(e) => {
                warn!(redemption_id = %redemption.redemption_id, error = %e, "1Click deposit/submit http failed (continuing)");
            }
        }

        // 4) Persist.
        mark_redemption_outbound(
            relay_store_path,
            &redemption.redemption_id,
            &tx_hex,
            &quote.correlation_id,
            &quote.quote.deposit_address,
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
                warn!(rid = %rid, error = %e, "1Click status http failed (continuing)");
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
                warn!(rid = %rid, error = %e, "1Click status decode failed");
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
}

struct PendingRedemption {
    redemption_id: String,
    basket_amount: String,
}

fn load_pending_intents(store_path: &str) -> Result<Vec<PendingIntent>> {
    let conn = Connection::open(store_path)?;
    let mut stmt = conn.prepare(
        r#"SELECT correlation_id, user_evm_addr, amount_in_wei
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
    // vault and the bridge-out note has been emitted (M4 work).
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
async fn process_outbound_b2agg(
    client: &mut miden_client::Client<FilesystemKeyStore>,
    relay_store_path: &str,
    relay_wallet: AccountId,
    bali_bridge: AccountId,
    bali_faucet: AccountId,
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
        // Same 30 bps redeem-fee net the legacy path applied. M4 will
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
                error!(redemption_id = %redemption.redemption_id, error = %e, "fungible asset build failed");
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
                error!(redemption_id = %redemption.redemption_id, error = %e, "note assets build failed");
                mark_redemption_error(
                    relay_store_path,
                    &redemption.redemption_id,
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
                error!(redemption_id = %redemption.redemption_id, error = %e, "B2AGG note build failed");
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
                error!(redemption_id = %redemption.redemption_id, error = %e, "B2AGG tx-request build failed");
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
                        error!(redemption_id = %redemption.redemption_id, error = %e, "B2AGG prove failed");
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
                        error!(redemption_id = %redemption.redemption_id, error = %e, "B2AGG submit failed");
                        mark_redemption_error(
                            relay_store_path,
                            &redemption.redemption_id,
                            &format!("B2AGG submit: {e}"),
                        )?;
                        continue;
                    }
                };
                if let Err(e) = client.apply_transaction(&result, height).await {
                    warn!(redemption_id = %redemption.redemption_id, error = %e, "B2AGG apply_transaction warning");
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
                error!(redemption_id = %redemption.redemption_id, error = %e, "B2AGG execute failed");
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
                error!(request_id = %row.request_id, error = %e, "fungible asset build failed");
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
                error!(request_id = %row.request_id, error = %e, "note assets build failed");
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
                error!(request_id = %row.request_id, error = %e, "B2AGG note build failed");
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
                error!(request_id = %row.request_id, error = %e, "B2AGG tx-request build failed");
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
                        error!(request_id = %row.request_id, error = %e, "B2AGG prove failed");
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
                        error!(request_id = %row.request_id, error = %e, "B2AGG submit failed");
                        mark_bridge_out_failed(
                            relay_store_path,
                            &row.request_id,
                            &format!("B2AGG submit: {e}"),
                        )?;
                        continue;
                    }
                };
                if let Err(e) = client.apply_transaction(&result, height).await {
                    warn!(request_id = %row.request_id, error = %e, "B2AGG apply_transaction warning");
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
                error!(request_id = %row.request_id, error = %e, "B2AGG execute failed");
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
        r#"SELECT redemption_id, user_evm_addr, basket_amount
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
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);
    drop(conn);

    if rows.is_empty() {
        return Ok(());
    }
    info!(count = rows.len(), "polling bali bridge service for L1 claims");

    for (redemption_id, user_evm_addr, basket_amount) in rows {
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
                warn!(redemption_id = %redemption_id, error = %e, "bali bridge service http failed");
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
                warn!(redemption_id = %redemption_id, error = %e, "bali bridge service decode failed");
                continue;
            }
        };
        let deposits = body.get("deposits").and_then(|d| d.as_array());
        let Some(deposits) = deposits else { continue };

        // Match by (dest_addr eq user) AND (amount eq expected) AND
        // (claim_tx_hash present). The bridge service indexes our
        // outbound burns under the L1 dest_addr; multiple redemptions
        // from the same user could be in flight, so we additionally
        // disambiguate by amount.
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
                  updated_at                 = ?6
              WHERE redemption_id = ?1"#,
        params![redemption_id, tx_hex, joined, cid, dep, now],
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
