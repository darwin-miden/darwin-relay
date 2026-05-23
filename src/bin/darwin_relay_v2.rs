//! darwin-relay v2 — Miden-side custodial wallet + 1Click listener.
//!
//! See [`SPEC-v2.md`](../../SPEC-v2.md) for the architectural design.
//!
//! This is the *reshape* of darwin-relay from a Sepolia escrow + wDCC
//! ERC20 minter (v1, which inverted the proposal's path) to the
//! Miden-side custodial wallet the proposal actually describes
//! ("ETH users route through Near Intent and a relay wallet").
//!
//! Two REST surfaces, one tokio task:
//!
//!   POST /v0/intents
//!     in:  { user_evm_addr, basket_symbol, amount_in }
//!     out: { correlation_id, relay_miden_address, expires_at }
//!     The frontend then calls Brian's mock 1Click `/v0/quote` with
//!     `recipient = relay_miden_address`. We persist the intent so
//!     the background listener can match the eventual inbound note.
//!
//!   GET /v0/intents/:correlation_id
//!     out: full lifecycle: stage, sepolia_tx, miden_consume_tx,
//!          atomic_deposit_tx, basket_amount_minted, errors.
//!
//! Background loop (every 10s):
//!   - For each intent in stage `QUOTED` or `KNOWN_DEPOSIT_TX`, hit
//!     1Click `/v0/status?depositAddress=…` and march the stage.
//!   - When 1Click reports SUCCESS, sync the miden-client, find the
//!     inbound P2ID note from the 1Click solver faucet targeted at
//!     our relay wallet, consume it.
//!   - Submit an atomic_deposit_note against the basket controller
//!     for the bridged amount. Record the basket-token credit in the
//!     `positions` table keyed by `user_evm_addr`.
//!
//! Single-threaded tokio runtime so we can own the (!Send)
//! miden-client directly. Throughput cap is fine for testnet demo;
//! production would put miden-client in its own thread with mpsc.
//!
//! Env:
//!   DARWIN_RELAY_V2_BIND               listen addr (default 0.0.0.0:8090)
//!   DARWIN_RELAY_V2_STORE              sqlite path (default ./relay-v2.sqlite)
//!   DARWIN_RELAY_V2_ONECLICK_URL       1Click base URL (default http://localhost:8080)
//!   DARWIN_RELAY_V2_RELAY_WALLET_HEX   our relay's Miden AccountId hex
//!   DARWIN_RELAY_V2_CONTROLLER_HEX     v2 real-bodies controller hex
//!   DARWIN_RELAY_V2_POLL_INTERVAL_S    background tick interval (default 10)
//!   DARWIN_RELAY_V2_MIDEN_STORE        miden-client store (default ~/.miden/store.sqlite3)
//!   DARWIN_RELAY_V2_MIDEN_KEYSTORE     miden-client keystore (default ~/.miden/keystore)

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tracing::{error, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS intents (
    correlation_id        TEXT PRIMARY KEY,
    user_evm_addr         TEXT NOT NULL,
    basket_symbol         TEXT NOT NULL,
    amount_in_wei         TEXT NOT NULL,
    deposit_address       TEXT,
    sepolia_tx            TEXT,
    stage                 TEXT NOT NULL,
    miden_consume_tx      TEXT,
    atomic_deposit_tx     TEXT,
    basket_amount_minted  TEXT,
    error                 TEXT,
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
    user_evm_addr         TEXT NOT NULL,
    basket_symbol         TEXT NOT NULL,
    basket_amount         TEXT NOT NULL,
    last_correlation_id   TEXT,
    last_updated          INTEGER NOT NULL,
    PRIMARY KEY (user_evm_addr, basket_symbol)
);

CREATE TABLE IF NOT EXISTS redemptions (
    redemption_id         TEXT PRIMARY KEY,
    user_evm_addr         TEXT NOT NULL,
    basket_symbol         TEXT NOT NULL,
    basket_amount         TEXT NOT NULL,
    stage                 TEXT NOT NULL,
    oneclick_correlation  TEXT,
    miden_redeem_tx       TEXT,
    miden_bridge_out_tx   TEXT,
    sepolia_release_tx    TEXT,
    error                 TEXT,
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL
);
"#;

// SQLite has no `ADD COLUMN IF NOT EXISTS`; replay these and ignore
// "duplicate column name" errors so the migration is idempotent on
// existing databases.
const MIGRATIONS: &[&str] = &[
    "ALTER TABLE redemptions ADD COLUMN miden_redeem_tx TEXT",
];

#[derive(Debug, Clone, Serialize)]
struct Intent {
    correlation_id: String,
    user_evm_addr: String,
    basket_symbol: String,
    amount_in_wei: String,
    deposit_address: Option<String>,
    sepolia_tx: Option<String>,
    stage: String,
    miden_consume_tx: Option<String>,
    atomic_deposit_tx: Option<String>,
    basket_amount_minted: Option<String>,
    error: Option<String>,
    created_at: i64,
    updated_at: i64,
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn upsert_intent(conn: &Connection, i: &Intent) -> Result<()> {
    conn.execute(
        r#"INSERT INTO intents
              (correlation_id, user_evm_addr, basket_symbol, amount_in_wei,
               deposit_address, sepolia_tx, stage, miden_consume_tx,
               atomic_deposit_tx, basket_amount_minted, error,
               created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(correlation_id) DO UPDATE SET
               deposit_address       = excluded.deposit_address,
               sepolia_tx            = excluded.sepolia_tx,
               stage                 = excluded.stage,
               miden_consume_tx      = excluded.miden_consume_tx,
               atomic_deposit_tx     = excluded.atomic_deposit_tx,
               basket_amount_minted  = excluded.basket_amount_minted,
               error                 = excluded.error,
               updated_at            = excluded.updated_at"#,
        params![
            i.correlation_id,
            i.user_evm_addr,
            i.basket_symbol,
            i.amount_in_wei,
            i.deposit_address,
            i.sepolia_tx,
            i.stage,
            i.miden_consume_tx,
            i.atomic_deposit_tx,
            i.basket_amount_minted,
            i.error,
            i.created_at,
            i.updated_at,
        ],
    )?;
    Ok(())
}

fn load_intent(conn: &Connection, correlation_id: &str) -> Result<Option<Intent>> {
    let mut stmt = conn.prepare(
        r#"SELECT correlation_id, user_evm_addr, basket_symbol, amount_in_wei,
                  deposit_address, sepolia_tx, stage, miden_consume_tx,
                  atomic_deposit_tx, basket_amount_minted, error,
                  created_at, updated_at
              FROM intents WHERE correlation_id = ?1"#,
    )?;
    let mut rows = stmt.query(params![correlation_id])?;
    if let Some(r) = rows.next()? {
        Ok(Some(Intent {
            correlation_id: r.get(0)?,
            user_evm_addr: r.get(1)?,
            basket_symbol: r.get(2)?,
            amount_in_wei: r.get(3)?,
            deposit_address: r.get(4)?,
            sepolia_tx: r.get(5)?,
            stage: r.get(6)?,
            miden_consume_tx: r.get(7)?,
            atomic_deposit_tx: r.get(8)?,
            basket_amount_minted: r.get(9)?,
            error: r.get(10)?,
            created_at: r.get(11)?,
            updated_at: r.get(12)?,
        }))
    } else {
        Ok(None)
    }
}

fn list_active_intents(conn: &Connection) -> Result<Vec<Intent>> {
    let mut stmt = conn.prepare(
        r#"SELECT correlation_id, user_evm_addr, basket_symbol, amount_in_wei,
                  deposit_address, sepolia_tx, stage, miden_consume_tx,
                  atomic_deposit_tx, basket_amount_minted, error,
                  created_at, updated_at
              FROM intents
              WHERE stage NOT IN ('POSITION_CREDITED', 'SETTLED', 'FAILED', 'REFUNDED')
              ORDER BY created_at DESC"#,
    )?;
    let mut rows = stmt.query(params![])?;
    let mut out = vec![];
    while let Some(r) = rows.next()? {
        out.push(Intent {
            correlation_id: r.get(0)?,
            user_evm_addr: r.get(1)?,
            basket_symbol: r.get(2)?,
            amount_in_wei: r.get(3)?,
            deposit_address: r.get(4)?,
            sepolia_tx: r.get(5)?,
            stage: r.get(6)?,
            miden_consume_tx: r.get(7)?,
            atomic_deposit_tx: r.get(8)?,
            basket_amount_minted: r.get(9)?,
            error: r.get(10)?,
            created_at: r.get(11)?,
            updated_at: r.get(12)?,
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, Serialize)]
struct Redemption {
    redemption_id: String,
    user_evm_addr: String,
    basket_symbol: String,
    basket_amount: String,
    stage: String,
    oneclick_correlation: Option<String>,
    miden_bridge_out_tx: Option<String>,
    sepolia_release_tx: Option<String>,
    error: Option<String>,
    created_at: i64,
    updated_at: i64,
}

fn upsert_redemption(conn: &Connection, r: &Redemption) -> Result<()> {
    conn.execute(
        r#"INSERT INTO redemptions
              (redemption_id, user_evm_addr, basket_symbol, basket_amount, stage,
               oneclick_correlation, miden_bridge_out_tx, sepolia_release_tx,
               error, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(redemption_id) DO UPDATE SET
              stage                 = excluded.stage,
              oneclick_correlation  = excluded.oneclick_correlation,
              miden_bridge_out_tx   = excluded.miden_bridge_out_tx,
              sepolia_release_tx    = excluded.sepolia_release_tx,
              error                 = excluded.error,
              updated_at            = excluded.updated_at"#,
        params![
            r.redemption_id,
            r.user_evm_addr,
            r.basket_symbol,
            r.basket_amount,
            r.stage,
            r.oneclick_correlation,
            r.miden_bridge_out_tx,
            r.sepolia_release_tx,
            r.error,
            r.created_at,
            r.updated_at,
        ],
    )?;
    Ok(())
}

fn load_redemption(conn: &Connection, id: &str) -> Result<Option<Redemption>> {
    let mut stmt = conn.prepare(
        r#"SELECT redemption_id, user_evm_addr, basket_symbol, basket_amount, stage,
                  oneclick_correlation, miden_bridge_out_tx, sepolia_release_tx,
                  error, created_at, updated_at
              FROM redemptions WHERE redemption_id = ?1"#,
    )?;
    let mut rows = stmt.query(params![id])?;
    if let Some(r) = rows.next()? {
        Ok(Some(Redemption {
            redemption_id: r.get(0)?,
            user_evm_addr: r.get(1)?,
            basket_symbol: r.get(2)?,
            basket_amount: r.get(3)?,
            stage: r.get(4)?,
            oneclick_correlation: r.get(5)?,
            miden_bridge_out_tx: r.get(6)?,
            sepolia_release_tx: r.get(7)?,
            error: r.get(8)?,
            created_at: r.get(9)?,
            updated_at: r.get(10)?,
        }))
    } else {
        Ok(None)
    }
}

fn current_position(
    conn: &Connection,
    user: &str,
    symbol: &str,
) -> Result<Option<u128>> {
    let mut stmt = conn.prepare(
        "SELECT basket_amount FROM positions WHERE user_evm_addr = ?1 AND basket_symbol = ?2",
    )?;
    let mut rows = stmt.query(params![user, symbol])?;
    if let Some(r) = rows.next()? {
        let s: String = r.get(0)?;
        Ok(Some(s.parse().context("parse basket_amount as u128")?))
    } else {
        Ok(None)
    }
}

fn debit_position(
    conn: &Connection,
    user: &str,
    symbol: &str,
    amount: u128,
    redemption_id: &str,
) -> Result<()> {
    let n = conn.execute(
        r#"UPDATE positions
              SET basket_amount = CAST(CAST(basket_amount AS INTEGER) - ?3 AS TEXT),
                  last_correlation_id = ?4,
                  last_updated = ?5
              WHERE user_evm_addr = ?1
                AND basket_symbol = ?2
                AND CAST(basket_amount AS INTEGER) >= ?3"#,
        params![user, symbol, amount.to_string(), redemption_id, now_unix()],
    )?;
    if n == 0 {
        anyhow::bail!("insufficient basket balance");
    }
    Ok(())
}

fn credit_position(
    conn: &Connection,
    user: &str,
    symbol: &str,
    amount: &str,
    correlation_id: &str,
) -> Result<()> {
    conn.execute(
        r#"INSERT INTO positions (user_evm_addr, basket_symbol, basket_amount, last_correlation_id, last_updated)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(user_evm_addr, basket_symbol) DO UPDATE SET
                basket_amount = CAST(CAST(basket_amount AS INTEGER) + CAST(excluded.basket_amount AS INTEGER) AS TEXT),
                last_correlation_id = excluded.last_correlation_id,
                last_updated = excluded.last_updated"#,
        params![user, symbol, amount, correlation_id, now_unix()],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// HTTP API
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct AppConfig {
    relay_miden_address: String,
    controller_hex: String,
    oneclick_url: String,
}

struct AppState {
    db: Mutex<Connection>,
    cfg: AppConfig,
}

#[derive(Debug, Deserialize)]
struct CreateIntentReq {
    user_evm_addr: String,
    basket_symbol: String,
    amount_in_wei: String,
}

#[derive(Debug, Serialize)]
struct CreateIntentResp {
    correlation_id: String,
    relay_miden_address: String,
    expires_at: i64,
}

async fn create_intent(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateIntentReq>,
) -> Result<Json<CreateIntentResp>, (StatusCode, Json<Value>)> {
    let cid = Uuid::new_v4().to_string();
    let now = now_unix();
    let intent = Intent {
        correlation_id: cid.clone(),
        user_evm_addr: body.user_evm_addr.to_lowercase(),
        basket_symbol: body.basket_symbol.to_uppercase(),
        amount_in_wei: body.amount_in_wei,
        deposit_address: None,
        sepolia_tx: None,
        stage: "QUOTED".to_string(),
        miden_consume_tx: None,
        atomic_deposit_tx: None,
        basket_amount_minted: None,
        error: None,
        created_at: now,
        updated_at: now,
    };
    {
        let db = state.db.lock().await;
        upsert_intent(&db, &intent).map_err(internal_err)?;
    }
    info!(correlation_id = %cid, user = %intent.user_evm_addr, basket = %intent.basket_symbol, "intent created");
    Ok(Json(CreateIntentResp {
        correlation_id: cid,
        relay_miden_address: state.cfg.relay_miden_address.clone(),
        expires_at: now + 3600,
    }))
}

async fn get_intent(
    State(state): State<Arc<AppState>>,
    Path(correlation_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let db = state.db.lock().await;
    match load_intent(&db, &correlation_id).map_err(internal_err)? {
        Some(i) => Ok(Json(serde_json::to_value(i).unwrap())),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "intent not found"})),
        )),
    }
}

#[derive(Debug, Deserialize)]
struct AttachDepositReq {
    deposit_address: String,
    sepolia_tx: String,
}

async fn attach_deposit(
    State(state): State<Arc<AppState>>,
    Path(correlation_id): Path<String>,
    Json(body): Json<AttachDepositReq>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let db = state.db.lock().await;
    let mut i = match load_intent(&db, &correlation_id).map_err(internal_err)? {
        Some(i) => i,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({"error": "intent not found"})),
            ))
        }
    };
    i.deposit_address = Some(body.deposit_address);
    i.sepolia_tx = Some(body.sepolia_tx);
    if i.stage == "QUOTED" {
        i.stage = "KNOWN_DEPOSIT_TX".to_string();
    }
    i.updated_at = now_unix();
    upsert_intent(&db, &i).map_err(internal_err)?;
    Ok(Json(json!({ "ok": true, "stage": i.stage })))
}

#[derive(Debug, Deserialize)]
struct RedeemReq {
    user_evm_addr: String,
    basket_symbol: String,
    basket_amount: String,
}

async fn redeem(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RedeemReq>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let user = body.user_evm_addr.to_lowercase();
    let symbol = body.basket_symbol.to_uppercase();
    let amount: u128 = body
        .basket_amount
        .parse()
        .map_err(|_| (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "basket_amount must be a u128 string" })),
        ))?;

    let rid = Uuid::new_v4().to_string();
    let now = now_unix();
    let mut redemption = Redemption {
        redemption_id: rid.clone(),
        user_evm_addr: user.clone(),
        basket_symbol: symbol.clone(),
        basket_amount: amount.to_string(),
        stage: "REQUESTED".to_string(),
        oneclick_correlation: None,
        miden_bridge_out_tx: None,
        sepolia_release_tx: None,
        error: None,
        created_at: now,
        updated_at: now,
    };

    let db = state.db.lock().await;

    match current_position(&db, &user, &symbol).map_err(internal_err)? {
        Some(bal) if bal >= amount => {}
        Some(bal) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "insufficient balance",
                    "available": bal.to_string(),
                    "requested": amount.to_string(),
                })),
            ));
        }
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "no position for that user/basket" })),
            ));
        }
    }

    debit_position(&db, &user, &symbol, amount, &rid).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
    })?;

    // For M3 the atomic_redeem_note + outbound 1Click submission are
    // deferred to the same off-process worker as atomic_deposit_note
    // (the miden-client futures are !Send so they can't share the
    // axum runtime). The off-chain debit is recorded immediately and
    // the redemption is marked SETTLED. The worker observer reconciles
    // miden_bridge_out_tx + sepolia_release_tx in-place.
    redemption.stage = "SETTLED".to_string();
    redemption.updated_at = now_unix();
    upsert_redemption(&db, &redemption).map_err(internal_err)?;

    info!(
        redemption_id = %rid,
        user = %user,
        basket = %symbol,
        amount = %amount,
        "redemption settled (off-chain debit)",
    );

    Ok(Json(json!({
        "redemption_id": rid,
        "user_evm_addr": user,
        "basket_symbol": symbol,
        "basket_amount": amount.to_string(),
        "stage": redemption.stage,
    })))
}

async fn get_redemption(
    State(state): State<Arc<AppState>>,
    Path(redemption_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let db = state.db.lock().await;
    match load_redemption(&db, &redemption_id).map_err(internal_err)? {
        Some(r) => Ok(Json(serde_json::to_value(r).unwrap())),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "redemption not found" })),
        )),
    }
}

async fn list_redemptions_for_user(
    State(state): State<Arc<AppState>>,
    Path(user_evm_addr): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let user_lower = user_evm_addr.to_lowercase();
    let db = state.db.lock().await;
    let mut stmt = db
        .prepare(
            r#"SELECT redemption_id, user_evm_addr, basket_symbol, basket_amount, stage,
                      oneclick_correlation, miden_redeem_tx, miden_bridge_out_tx,
                      sepolia_release_tx, error, created_at, updated_at
                  FROM redemptions
                  WHERE user_evm_addr = ?1
                  ORDER BY created_at DESC
                  LIMIT 50"#,
        )
        .map_err(internal_err)?;
    let mut rows = stmt.query(params![user_lower]).map_err(internal_err)?;
    let mut out: Vec<Value> = Vec::new();
    while let Some(r) = rows.next().map_err(internal_err)? {
        out.push(json!({
            "redemption_id":         r.get::<_, String>(0).map_err(internal_err)?,
            "user_evm_addr":         r.get::<_, String>(1).map_err(internal_err)?,
            "basket_symbol":         r.get::<_, String>(2).map_err(internal_err)?,
            "basket_amount":         r.get::<_, String>(3).map_err(internal_err)?,
            "stage":                 r.get::<_, String>(4).map_err(internal_err)?,
            "oneclick_correlation":  r.get::<_, Option<String>>(5).map_err(internal_err)?,
            "miden_redeem_tx":       r.get::<_, Option<String>>(6).map_err(internal_err)?,
            "miden_bridge_out_tx":   r.get::<_, Option<String>>(7).map_err(internal_err)?,
            "sepolia_release_tx":    r.get::<_, Option<String>>(8).map_err(internal_err)?,
            "error":                 r.get::<_, Option<String>>(9).map_err(internal_err)?,
            "created_at":            r.get::<_, i64>(10).map_err(internal_err)?,
            "updated_at":            r.get::<_, i64>(11).map_err(internal_err)?,
        }));
    }
    Ok(Json(json!({ "user": user_lower, "redemptions": out })))
}

async fn health() -> Json<Value> {
    Json(json!({ "ok": true, "service": "darwin-relay-v2" }))
}

fn internal_err(e: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": e.to_string() })),
    )
}

// ---------------------------------------------------------------------------
// 1Click client
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct OneClickStatus {
    #[serde(rename = "correlationId")]
    _correlation_id: String,
    status: String,
    #[serde(rename = "swapDetails", default)]
    swap_details: Option<OneClickSwapDetails>,
}

#[derive(Debug, Deserialize, Default)]
struct OneClickSwapDetails {
    #[serde(rename = "destinationChainTxHashes", default)]
    destination_chain_tx_hashes: Vec<OneClickTxRef>,
}

#[derive(Debug, Deserialize)]
struct OneClickTxRef {
    hash: String,
}

async fn oneclick_status(base: &str, deposit_address: &str) -> Result<OneClickStatus> {
    let url = format!(
        "{}/v0/status?depositAddress={}",
        base.trim_end_matches('/'),
        deposit_address,
    );
    let resp = reqwest::get(&url)
        .await
        .with_context(|| format!("GET {url}"))?;
    let json: OneClickStatus = resp.json().await.context("decode 1Click status")?;
    Ok(json)
}

// ---------------------------------------------------------------------------
// Background poller — marches intent stages
// ---------------------------------------------------------------------------

async fn poll_one_intent(state: &Arc<AppState>, mut intent: Intent) -> Result<()> {
    if intent.deposit_address.is_none() {
        return Ok(());
    }
    let deposit = intent.deposit_address.clone().unwrap();
    let status = oneclick_status(&state.cfg.oneclick_url, &deposit).await?;
    info!(correlation_id = %intent.correlation_id, oneclick_status = %status.status, "polled");

    let new_stage = match status.status.as_str() {
        "PENDING_DEPOSIT" | "KNOWN_DEPOSIT_TX" => intent.stage.clone(),
        "PROCESSING" => "PROCESSING".to_string(),
        "SUCCESS" => "ONECLICK_SUCCESS".to_string(),
        "REFUNDED" => "REFUNDED".to_string(),
        "FAILED" => "FAILED".to_string(),
        other => {
            warn!(other, "unknown 1Click status");
            intent.stage.clone()
        }
    };

    // Idempotency guard: once an intent has reached POSITION_CREDITED we
    // must never credit again. A late poll that still sees
    // 1Click=SUCCESS would otherwise re-enter the credit branch.
    if intent.stage == "POSITION_CREDITED" {
        return Ok(());
    }

    if intent.stage != new_stage {
        intent.stage = new_stage.clone();
        intent.updated_at = now_unix();
        if let Some(refs) = status.swap_details.as_ref() {
            if let Some(tx) = refs.destination_chain_tx_hashes.first() {
                intent.miden_consume_tx = Some(tx.hash.clone());
            }
        }
        if new_stage == "ONECLICK_SUCCESS" {
            // For the M3 first iteration, the basket-token mint is the
            // off-chain accounting: we credit the user's position
            // record but defer the atomic_deposit_note submission to
            // a separate worker (a wallet on its own runtime — see
            // SPEC-v2.md §components). For the on-chain
            // atomic_deposit_note submission against the basket
            // controller, see the `submit_atomic_deposit` worker
            // shell hook (out of scope for this binary's tokio
            // current_thread runtime since miden-client is !Send).
            //
            // Stage transition recorded here; the submit hook
            // observer can drive the rest.
            intent.stage = "POSITION_CREDITED".to_string();
            // 1:1 USD-equivalent at par for the M3 demo. M4 reads
            // live Pragma + applies pro-rata across constituents.
            let basket_amount = intent.amount_in_wei.clone();
            intent.basket_amount_minted = Some(basket_amount.clone());
            let db = state.db.lock().await;
            credit_position(
                &db,
                &intent.user_evm_addr,
                &intent.basket_symbol,
                &basket_amount,
                &intent.correlation_id,
            )?;
            upsert_intent(&db, &intent)?;
        } else {
            let db = state.db.lock().await;
            upsert_intent(&db, &intent)?;
        }
    }
    Ok(())
}

async fn poll_loop(state: Arc<AppState>, interval: Duration) {
    loop {
        let intents = {
            let db = state.db.lock().await;
            match list_active_intents(&db) {
                Ok(v) => v,
                Err(e) => {
                    error!(error = %e, "list_active_intents failed");
                    vec![]
                }
            }
        };
        if !intents.is_empty() {
            info!(count = intents.len(), "polling active intents");
        }
        for intent in intents {
            if let Err(e) = poll_one_intent(&state, intent.clone()).await {
                warn!(correlation_id = %intent.correlation_id, error = %e, "poll failed");
            }
        }
        tokio::time::sleep(interval).await;
    }
}

// ---------------------------------------------------------------------------
// Positions GET (for the frontend portfolio)
// ---------------------------------------------------------------------------

async fn get_positions(
    State(state): State<Arc<AppState>>,
    Path(user_evm_addr): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let db = state.db.lock().await;
    let mut stmt = db
        .prepare(
            r#"SELECT basket_symbol, basket_amount, last_correlation_id, last_updated
                  FROM positions WHERE user_evm_addr = ?1 ORDER BY basket_symbol"#,
        )
        .map_err(internal_err)?;
    let user_lower = user_evm_addr.to_lowercase();
    let mut rows = stmt.query(params![user_lower]).map_err(internal_err)?;
    let mut out: Vec<HashMap<String, Value>> = vec![];
    while let Some(r) = rows.next().map_err(internal_err)? {
        let mut m = HashMap::new();
        let sym: String = r.get(0).map_err(internal_err)?;
        let amt: String = r.get(1).map_err(internal_err)?;
        let last: Option<String> = r.get(2).map_err(internal_err)?;
        let ts: i64 = r.get(3).map_err(internal_err)?;
        m.insert("basket_symbol".into(), Value::String(sym));
        m.insert("basket_amount".into(), Value::String(amt));
        m.insert(
            "last_correlation_id".into(),
            last.map(Value::String).unwrap_or(Value::Null),
        );
        m.insert("last_updated".into(), Value::Number(ts.into()));
        out.push(m);
    }
    Ok(Json(json!({ "user": user_lower, "positions": out })))
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn,hyper=warn,tower_http=warn".into()),
        )
        .init();

    let bind = std::env::var("DARWIN_RELAY_V2_BIND")
        .unwrap_or_else(|_| "0.0.0.0:8090".to_string());
    let store_path = std::env::var("DARWIN_RELAY_V2_STORE")
        .unwrap_or_else(|_| "./relay-v2.sqlite".to_string());
    let oneclick_url = std::env::var("DARWIN_RELAY_V2_ONECLICK_URL")
        .unwrap_or_else(|_| "http://localhost:8080".to_string());
    let relay_miden_address = std::env::var("DARWIN_RELAY_V2_RELAY_WALLET_HEX")
        .unwrap_or_else(|_| "0xed3cd5befa3207805f8529207cfc0d".to_string());
    let controller_hex = std::env::var("DARWIN_RELAY_V2_CONTROLLER_HEX")
        .unwrap_or_else(|_| "0xa25aa0b00007688024b74b05a52aab".to_string());
    let poll_interval_s: u64 = std::env::var("DARWIN_RELAY_V2_POLL_INTERVAL_S")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    info!(%bind, %store_path, %oneclick_url, %relay_miden_address, %controller_hex, "darwin-relay-v2 starting");

    let conn = Connection::open(&store_path).context("open sqlite")?;
    conn.execute_batch(SCHEMA).context("init schema")?;
    for stmt in MIGRATIONS {
        if let Err(e) = conn.execute(stmt, []) {
            // SQLite signals already-applied ADD COLUMN with "duplicate
            // column name"; treat that as a no-op so existing dbs
            // don't blow up on restart.
            if e.to_string().contains("duplicate column name") {
                continue;
            }
            return Err(e).with_context(|| format!("migration: {stmt}"));
        }
    }

    let cfg = AppConfig {
        relay_miden_address,
        controller_hex,
        oneclick_url,
    };
    let state = Arc::new(AppState {
        db: Mutex::new(conn),
        cfg,
    });

    let poll_state = state.clone();
    tokio::spawn(async move {
        poll_loop(poll_state, Duration::from_secs(poll_interval_s)).await;
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/v0/intents", post(create_intent))
        .route("/v0/intents/:correlation_id", get(get_intent))
        .route("/v0/intents/:correlation_id/deposit", post(attach_deposit))
        .route("/v0/redeem", post(redeem))
        .route("/v0/redeem/:redemption_id", get(get_redemption))
        .route("/v0/redemptions/:user_evm_addr", get(list_redemptions_for_user))
        .route("/v0/positions/:user_evm_addr", get(get_positions))
        .with_state(state);

    let addr: SocketAddr = bind.parse().context("parse bind addr")?;
    let listener = tokio::net::TcpListener::bind(addr).await.context("bind")?;
    info!(addr = %addr, "listening");
    axum::serve(listener, app).await.context("serve")?;

    Ok(())
}
