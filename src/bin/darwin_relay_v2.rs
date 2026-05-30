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
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use rusqlite::{params, Connection, OptionalExtension};
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

-- Worker heartbeat — the worker writes one row per binary at the end
-- of every tick; REST reads from /v0/worker-health so operators can
-- detect a stuck worker without log scraping. Single row per worker_id
-- (typically just "main" since there's exactly one worker process).
CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_id        TEXT PRIMARY KEY,
    last_tick_at     INTEGER NOT NULL,
    last_tick_status TEXT NOT NULL
);

-- Direct canonical Bali outbound requests (no basket position
-- involved). REST writes the row; the worker drains, builds a
-- B2AggNote from the relay vault toward dest_address, and writes
-- the Miden tx id back. The frontend's BaliWithdrawPanel polls the
-- GET endpoint for status.
CREATE TABLE IF NOT EXISTS pending_bridge_outs (
    request_id    TEXT PRIMARY KEY,
    dest_address  TEXT NOT NULL,
    amount        TEXT NOT NULL,
    status        TEXT NOT NULL,
    miden_tx_id   TEXT,
    error         TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);
"#;

// SQLite has no `ADD COLUMN IF NOT EXISTS`; replay these and ignore
// "duplicate column name" errors so the migration is idempotent on
// existing databases.
const MIGRATIONS: &[&str] = &[
    "ALTER TABLE redemptions ADD COLUMN miden_redeem_tx TEXT",
    // Split the legacy `oneclick_correlation` joint-by-pipe ('<id>|<addr>')
    // into two well-typed columns. Reads consume the new columns
    // (NULL = pure canonical B2AGG, populated = legacy 1Click mock
    // path). Legacy column is still written for backward compat
    // during the cutover.
    "ALTER TABLE redemptions ADD COLUMN oneclick_correlation_id TEXT",
    "ALTER TABLE redemptions ADD COLUMN oneclick_deposit_address TEXT",
    // Captures the highest deposit_cnt that exists on the Bali bridge
    // service for this user AT THE MOMENT we emit the b2agg burn.
    // The claim-watcher's status poller then only considers entries
    // with cnt > baseline so historical claimed burns with the same
    // amount don't get falsely attributed to this redemption.
    "ALTER TABLE redemptions ADD COLUMN bali_baseline_cnt INTEGER",
    // One-shot backfill of pre-split rows. Idempotent via the
    // `oneclick_correlation_id IS NULL` guard so future startups
    // don't clobber freshly-written values.
    r#"UPDATE redemptions
          SET oneclick_correlation_id  = NULLIF(SUBSTR(oneclick_correlation, 1, INSTR(oneclick_correlation, '|') - 1), ''),
              oneclick_deposit_address = NULLIF(SUBSTR(oneclick_correlation, INSTR(oneclick_correlation, '|') + 1), '')
        WHERE oneclick_correlation_id IS NULL
          AND oneclick_correlation IS NOT NULL
          AND INSTR(oneclick_correlation, '|') > 0"#,
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
    oneclick_url: String,
    // Tunables — read once at startup from env, default to the values
    // that shipped with M3. Surfacing them here keeps the magic numbers
    // out of the call sites and makes M4 fee changes a config flip
    // instead of a code change.
    wei_per_miden_base: u128,  // EVM 18-dec → Miden 8-dec base unit
    intent_expiry_s: i64,      // /v0/intents expires_at offset
    list_limit: usize,         // /v0/redemptions/:addr default LIMIT
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
    let user = body.user_evm_addr.trim().to_lowercase();
    let symbol = body.basket_symbol.trim().to_uppercase();
    let amount_wei = body.amount_in_wei.trim().to_string();
    if !is_valid_evm_addr(&user) {
        return Err(bad_request("user_evm_addr must be 0x + 40 hex"));
    }
    if !is_known_basket(&symbol) {
        return Err(bad_request("basket_symbol must be one of DCC, DAG, DCO, DPP"));
    }
    if !is_positive_u128(&amount_wei) {
        return Err(bad_request("amount_in_wei must be a positive integer string"));
    }

    let cid = Uuid::new_v4().to_string();
    let now = now_unix();
    let intent = Intent {
        correlation_id: cid.clone(),
        user_evm_addr: user,
        basket_symbol: symbol,
        amount_in_wei: amount_wei,
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
        expires_at: now + state.cfg.intent_expiry_s,
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
    let deposit_address = body.deposit_address.trim().to_string();
    let sepolia_tx = body.sepolia_tx.trim().to_string();
    if !is_valid_evm_addr(&deposit_address) {
        return Err(bad_request("deposit_address must be 0x + 40 hex"));
    }
    if !is_valid_tx_hash(&sepolia_tx) {
        return Err(bad_request("sepolia_tx must be 0x + 64 hex"));
    }
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
    i.deposit_address = Some(deposit_address);
    i.sepolia_tx = Some(sepolia_tx);
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
    let user = body.user_evm_addr.trim().to_lowercase();
    let symbol = body.basket_symbol.trim().to_uppercase();
    let basket_amount_str = body.basket_amount.trim().to_string();
    if !is_valid_evm_addr(&user) {
        return Err(bad_request("user_evm_addr must be 0x + 40 hex"));
    }
    if !is_known_basket(&symbol) {
        return Err(bad_request("basket_symbol must be one of DCC, DAG, DCO, DPP"));
    }
    if !is_positive_u128(&basket_amount_str) {
        return Err(bad_request("basket_amount must be a positive integer string"));
    }
    let amount: u128 = basket_amount_str.parse().expect("checked by is_positive_u128");

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

#[derive(Debug, Deserialize)]
struct ListRedemptionsQuery {
    /// created_at (unix-seconds) ceiling. Returns rows where
    /// `created_at < cursor`. Together with the page's last row's
    /// created_at, lets the client walk the history with stable
    /// ordering even when new redemptions land at the head.
    cursor: Option<i64>,
    /// page size override, clamped to [1, list_limit].
    limit: Option<usize>,
}

async fn list_redemptions_for_user(
    State(state): State<Arc<AppState>>,
    Path(user_evm_addr): Path<String>,
    Query(q): Query<ListRedemptionsQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let user_lower = user_evm_addr.to_lowercase();
    let limit = q
        .limit
        .unwrap_or(state.cfg.list_limit)
        .clamp(1, state.cfg.list_limit);
    // i64::MAX as default = "no ceiling" (= first page). Subsequent
    // calls pass the previous page's last created_at as the cursor.
    let cursor = q.cursor.unwrap_or(i64::MAX);
    let db = state.db.lock().await;
    // LIMIT is interpolated (sqlite won't bind it). Both interpolated
    // values are typed integers, so no SQL injection surface.
    let sql = format!(
        r#"SELECT redemption_id, user_evm_addr, basket_symbol, basket_amount, stage,
                  oneclick_correlation, miden_redeem_tx, miden_bridge_out_tx,
                  sepolia_release_tx, error, created_at, updated_at
              FROM redemptions
              WHERE user_evm_addr = ?1
                AND created_at    < ?2
              ORDER BY created_at DESC
              LIMIT {}"#,
        limit,
    );
    let mut stmt = db.prepare(&sql).map_err(internal_err)?;
    let mut rows = stmt
        .query(params![user_lower, cursor])
        .map_err(internal_err)?;
    let mut out: Vec<Value> = Vec::new();
    let mut last_created_at: Option<i64> = None;
    while let Some(r) = rows.next().map_err(internal_err)? {
        let created_at = r.get::<_, i64>(10).map_err(internal_err)?;
        last_created_at = Some(created_at);
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
            "created_at":            created_at,
            "updated_at":            r.get::<_, i64>(11).map_err(internal_err)?,
        }));
    }
    // `next_cursor` = the smallest created_at on this page. When the
    // page came back shorter than `limit`, there's no more history and
    // we return null so the client can stop polling.
    let next_cursor = if out.len() == limit { last_created_at } else { None };
    Ok(Json(json!({
        "user":        user_lower,
        "redemptions": out,
        "next_cursor": next_cursor,
        "limit":       limit,
    })))
}

// ---------------------------------------------------------------------------
// Direct canonical Bali outbound (`/v0/bridge-out`)
//
// REST half of the BaliWithdrawPanel: the user posts a destAddress +
// amount, the REST inserts a `pending_bridge_outs` row, and returns a
// request_id immediately. The worker drains the row, builds + submits
// the B2AggNote from the relay vault, and writes the Miden tx id back.
// The frontend polls the GET endpoint for status.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct BridgeOutReq {
    #[serde(rename = "destAddress")]
    dest_address: String,
    amount: String,
}

async fn create_bridge_out(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BridgeOutReq>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // 0x + 40 hex chars, like every other EVM address we accept.
    let dest = body.dest_address.trim().to_string();
    let dest_ok = dest.len() == 42
        && dest.starts_with("0x")
        && dest[2..].chars().all(|c| c.is_ascii_hexdigit());
    if !dest_ok {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "destAddress must be a 20-byte hex (0x + 40 chars)" })),
        ));
    }

    // Positive integer string, parseable as u128 (we store as text but
    // the worker re-parses to u64; reject early so a bad payload never
    // hits the burn path).
    let amount_str = body.amount.trim().to_string();
    let amount_ok = !amount_str.is_empty()
        && amount_str.chars().all(|c| c.is_ascii_digit())
        && amount_str.parse::<u128>().map(|n| n > 0).unwrap_or(false);
    if !amount_ok {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "amount must be a positive integer string" })),
        ));
    }

    let request_id = format!("bo-{}", Uuid::new_v4());
    let now = now_unix();
    {
        let db = state.db.lock().await;
        db.execute(
            r#"INSERT INTO pending_bridge_outs
                  (request_id, dest_address, amount, status,
                   miden_tx_id, error, created_at, updated_at)
                VALUES (?1, ?2, ?3, 'pending', NULL, NULL, ?4, ?4)"#,
            params![request_id, dest, amount_str, now],
        )
        .map_err(internal_err)?;
    }

    info!(
        request_id = %request_id,
        dest = %dest,
        amount = %amount_str,
        "bridge-out request enqueued",
    );

    Ok(Json(json!({
        "request_id": request_id,
        "status": "pending",
    })))
}

async fn get_bridge_out(
    State(state): State<Arc<AppState>>,
    Path(request_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let db = state.db.lock().await;
    let row = db
        .query_row(
            r#"SELECT dest_address, amount, status, miden_tx_id, error,
                      created_at, updated_at
                  FROM pending_bridge_outs
                  WHERE request_id = ?1"#,
            params![request_id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, i64>(5)?,
                    r.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()
        .map_err(internal_err)?;

    match row {
        Some((dest, amount, status, miden_tx, err, created_at, updated_at)) => {
            // Mirror the txId field name the frontend uses for the
            // success path so the panel can read `j.txId` directly
            // without two different shapes.
            let ok = status == "submitted";
            Ok(Json(json!({
                "request_id":   request_id,
                "dest_address": dest,
                "amount":       amount,
                "status":       status,
                "miden_tx_id":  miden_tx,
                "txId":         miden_tx,
                "ok":           ok,
                "error":        err,
                "created_at":   created_at,
                "updated_at":   updated_at,
            })))
        }
        None => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "bridge-out request not found" })),
        )),
    }
}

async fn worker_health(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let db = state.db.lock().await;
    let row = db
        .query_row(
            r#"SELECT worker_id, last_tick_at, last_tick_status
                  FROM worker_heartbeats
                  WHERE worker_id = 'main'"#,
            [],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(internal_err)?;

    match row {
        Some((worker_id, last_tick_at, last_tick_status)) => {
            let lag_s = now_unix() - last_tick_at;
            // ~3x the worker poll interval is the reasonable health
            // ceiling. The worker default is 30s ; default ceiling
            // 90s. Surfaces a clear `ok: false` so an upstream probe
            // can alert on it.
            let ok = lag_s <= 90 && last_tick_status == "ok";
            Ok(Json(json!({
                "ok":               ok,
                "worker_id":        worker_id,
                "last_tick_at":     last_tick_at,
                "last_tick_lag_s":  lag_s,
                "last_tick_status": last_tick_status,
            })))
        }
        None => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ok":    false,
                "error": "no worker heartbeat recorded — worker hasn't ticked yet or store is wrong",
            })),
        )),
    }
}

async fn health() -> Json<Value> {
    Json(json!({ "ok": true, "service": "darwin-relay-v2" }))
}

// Plain-text Prometheus exposition. Pulled live from sqlite so the
// service can run a single instance — no in-memory counter to lose
// on restart.
async fn metrics(State(state): State<Arc<AppState>>) -> (StatusCode, String) {
    let db = state.db.lock().await;

    let intent_stages = match db
        .prepare("SELECT stage, COUNT(*) FROM intents GROUP BY stage")
        .and_then(|mut stmt| {
            stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
                .collect::<rusqlite::Result<Vec<(String, i64)>>>()
        }) {
        Ok(rows) => rows,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("# metrics query failed: {e}\n"),
            )
        }
    };
    let redemption_stages = match db
        .prepare("SELECT stage, COUNT(*) FROM redemptions GROUP BY stage")
        .and_then(|mut stmt| {
            stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
                .collect::<rusqlite::Result<Vec<(String, i64)>>>()
        }) {
        Ok(rows) => rows,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("# metrics query failed: {e}\n"),
            )
        }
    };
    let position_count: i64 = db
        .query_row("SELECT COUNT(*) FROM positions", [], |r| r.get(0))
        .unwrap_or(0);

    let mut out = String::with_capacity(1024);
    out.push_str("# HELP darwin_relay_intents_total Intents grouped by stage.\n");
    out.push_str("# TYPE darwin_relay_intents_total gauge\n");
    for (stage, n) in &intent_stages {
        out.push_str(&format!(
            "darwin_relay_intents_total{{stage=\"{}\"}} {}\n",
            escape_label(stage),
            n
        ));
    }
    out.push_str("# HELP darwin_relay_redemptions_total Redemptions grouped by stage.\n");
    out.push_str("# TYPE darwin_relay_redemptions_total gauge\n");
    for (stage, n) in &redemption_stages {
        out.push_str(&format!(
            "darwin_relay_redemptions_total{{stage=\"{}\"}} {}\n",
            escape_label(stage),
            n
        ));
    }
    out.push_str("# HELP darwin_relay_positions Distinct (user, basket) positions tracked.\n");
    out.push_str("# TYPE darwin_relay_positions gauge\n");
    out.push_str(&format!("darwin_relay_positions {position_count}\n"));

    out.push_str("# HELP darwin_relay_up 1 if the service is alive.\n");
    out.push_str("# TYPE darwin_relay_up gauge\n");
    out.push_str("darwin_relay_up 1\n");

    (StatusCode::OK, out)
}

fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn internal_err(e: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": e.to_string() })),
    )
}

fn bad_request(msg: impl Into<String>) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": msg.into() })),
    )
}

// 0x + 40 ASCII hex chars. Same check as `/v0/bridge-out` so every
// REST entry point accepts the same address shape — no silent
// downstream surprises from a malformed EVM address persisted as-is.
fn is_valid_evm_addr(s: &str) -> bool {
    s.len() == 42
        && s.starts_with("0x")
        && s[2..].chars().all(|c| c.is_ascii_hexdigit())
}

// Sepolia / Ethereum tx hash: 0x + 64 hex chars.
fn is_valid_tx_hash(s: &str) -> bool {
    s.len() == 66
        && s.starts_with("0x")
        && s[2..].chars().all(|c| c.is_ascii_hexdigit())
}

// Positive integer string, parseable as u128. Stored as TEXT in
// sqlite but every consumer re-parses to u64/u128 — reject the bad
// shape at the door instead of letting it land in the ledger.
fn is_positive_u128(s: &str) -> bool {
    !s.is_empty()
        && s.chars().all(|c| c.is_ascii_digit())
        && s.parse::<u128>().map(|n| n > 0).unwrap_or(false)
}

// M3 baskets only (matches lib/baskets.ts on the frontend). New
// baskets land here as they ship.
fn is_known_basket(s: &str) -> bool {
    matches!(s, "DCC" | "DAG" | "DCO" | "DPP")
}

// ---------------------------------------------------------------------------
// 1Click client
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct OneClickStatus {
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
            // The frontend sends amount_in_wei in EVM 18-decimal wei.
            // The Miden side (dETH faucet, controller slot-10 credit)
            // operates in the 8-decimal convention, so the worker
            // scales the on-chain deposit by ÷10^10. Mirror that here
            // so the off-chain ledger (RelayPositionsPanel) shows the
            // same base-unit quantity the on-chain atomic_deposit
            // credits to the controller's slot 10 — otherwise the two
            // portfolio panels diverge by 10 orders of magnitude.
            // 1:1 USD-equivalent at par for the M3 demo; M4 reads live
            // Pragma + applies pro-rata across constituents.
            let wei_per_miden_base = state.cfg.wei_per_miden_base;
            let basket_amount = intent
                .amount_in_wei
                .parse::<u128>()
                .map(|wei| (wei / wei_per_miden_base).max(1).to_string())
                .unwrap_or_else(|_| intent.amount_in_wei.clone());
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
    // v6 fee-routing controller — strict superset of v5. Must match
    // the worker's DEFAULT_CONTROLLER_HEX or env var or the two sides
    // target different accounts and the on-chain slot-10 ledger never
    // updates.
    let controller_hex = std::env::var("DARWIN_RELAY_V2_CONTROLLER_HEX")
        .unwrap_or_else(|_| "0x2a3ea0a268d97b80497d6a966e3141".to_string());
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

    let wei_per_miden_base: u128 = std::env::var("DARWIN_RELAY_V2_WEI_PER_MIDEN_BASE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000_000_000);
    let intent_expiry_s: i64 = std::env::var("DARWIN_RELAY_V2_INTENT_EXPIRY_S")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3600);
    let list_limit: usize = std::env::var("DARWIN_RELAY_V2_LIST_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    let cfg = AppConfig {
        relay_miden_address,
        oneclick_url,
        wei_per_miden_base,
        intent_expiry_s,
        list_limit,
    };
    info!(
        wei_per_miden_base,
        intent_expiry_s,
        list_limit,
        "tunables loaded",
    );
    // controller_hex is logged for ops visibility but the REST handlers
    // never use it — the worker is what submits against the controller.
    let _ = controller_hex;
    let state = Arc::new(AppState {
        db: Mutex::new(conn),
        cfg,
    });

    let poll_state = state.clone();
    tokio::spawn(async move {
        poll_loop(poll_state, Duration::from_secs(poll_interval_s)).await;
    });

    // CORS — the frontend calls this REST directly from the browser
    // (OneClickDepositPanel POSTs /v0/intents, RelayRedemptionsPanel
    // GETs /v0/redemptions, etc.) from a different origin
    // (localhost:3010 in dev, darwin.xyz in prod). Without an
    // Access-Control-Allow-Origin header the browser blocks the
    // preflight and the panels show "Failed to fetch". Allow any
    // origin + the methods/headers the panels use — this is a public
    // read/write testnet relay, no credentials or cookies involved.
    let cors = tower_http::cors::CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any);

    let app = Router::new()
        .route("/health", get(health))
        .route("/v0/worker-health", get(worker_health))
        .route("/metrics", get(metrics))
        .route("/v0/intents", post(create_intent))
        .route("/v0/intents/:correlation_id", get(get_intent))
        .route("/v0/intents/:correlation_id/deposit", post(attach_deposit))
        .route("/v0/redeem", post(redeem))
        .route("/v0/redeem/:redemption_id", get(get_redemption))
        .route("/v0/redemptions/:user_evm_addr", get(list_redemptions_for_user))
        .route("/v0/positions/:user_evm_addr", get(get_positions))
        .route("/v0/bridge-out", post(create_bridge_out))
        .route("/v0/bridge-out/:request_id", get(get_bridge_out))
        .layer(cors)
        .with_state(state);

    let addr: SocketAddr = bind.parse().context("parse bind addr")?;
    let listener = tokio::net::TcpListener::bind(addr).await.context("bind")?;
    info!(addr = %addr, "listening");
    axum::serve(listener, app).await.context("serve")?;

    Ok(())
}
