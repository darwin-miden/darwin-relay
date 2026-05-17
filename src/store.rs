//! SQLite-backed persistence for deposit records. Crash-safe: if the
//! relay process restarts mid-flight, every deposit is recovered with
//! its current status, and the resume loop picks each non-terminal
//! one up where it left off.
//!
//! Schema lives in a single embedded migration (see `INIT_SQL`). We
//! deliberately don't use a heavyweight ORM — the deposit FSM has at
//! most a handful of columns and the data plane is small.

use anyhow::{anyhow, Context, Result};
use rusqlite::{params, Connection, OpenFlags};
use std::path::Path;

use crate::state::{DepositRecord, DepositStatus};

const INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS deposits (
    id                    INTEGER PRIMARY KEY,
    user_eth              TEXT NOT NULL,
    basket_id             TEXT NOT NULL,
    miden_recipient       TEXT NOT NULL,
    amount_usdc           TEXT NOT NULL,   -- u128 as decimal string
    status                TEXT NOT NULL,
    requested_at_unix     INTEGER NOT NULL,
    last_event_unix       INTEGER NOT NULL,
    claim_tx              TEXT,
    bridge_tx             TEXT,
    miden_consume_tx      TEXT,
    erc20_mint_tx         TEXT,
    confirm_tx            TEXT,
    refund_tx             TEXT,
    basket_amount_minted  TEXT,           -- u128 as decimal string
    failure_reason        TEXT
);

CREATE INDEX IF NOT EXISTS deposits_status_idx ON deposits(status);
"#;

pub struct DepositStore {
    conn: Connection,
}

impl DepositStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        conn.execute_batch(INIT_SQL)?;
        Ok(Self { conn })
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(INIT_SQL)?;
        Ok(Self { conn })
    }

    pub fn insert(&self, r: &DepositRecord) -> Result<()> {
        self.conn
            .execute(
                r#"INSERT INTO deposits (
                    id, user_eth, basket_id, miden_recipient, amount_usdc,
                    status, requested_at_unix, last_event_unix
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"#,
                params![
                    r.id as i64,
                    r.user_eth,
                    r.basket_id,
                    r.miden_recipient,
                    r.amount_usdc.to_string(),
                    r.status.as_str(),
                    r.requested_at_unix,
                    r.last_event_unix,
                ],
            )
            .with_context(|| format!("insert deposit {}", r.id))?;
        Ok(())
    }

    pub fn update_status(&self, id: u64, status: DepositStatus, now_unix: i64) -> Result<()> {
        let n = self.conn.execute(
            r#"UPDATE deposits SET status = ?1, last_event_unix = ?2 WHERE id = ?3"#,
            params![status.as_str(), now_unix, id as i64],
        )?;
        if n == 0 {
            return Err(anyhow!("update_status on unknown deposit {id}"));
        }
        Ok(())
    }

    pub fn set_tx(&self, id: u64, column: TxColumn, tx_hash: &str, now_unix: i64) -> Result<()> {
        let col = match column {
            TxColumn::Claim => "claim_tx",
            TxColumn::Bridge => "bridge_tx",
            TxColumn::MidenConsume => "miden_consume_tx",
            TxColumn::Erc20Mint => "erc20_mint_tx",
            TxColumn::Confirm => "confirm_tx",
            TxColumn::Refund => "refund_tx",
        };
        let sql = format!("UPDATE deposits SET {col} = ?1, last_event_unix = ?2 WHERE id = ?3");
        let n = self
            .conn
            .execute(&sql, params![tx_hash, now_unix, id as i64])?;
        if n == 0 {
            return Err(anyhow!("set_tx({col}) on unknown deposit {id}"));
        }
        Ok(())
    }

    pub fn get(&self, id: u64) -> Result<Option<DepositRecord>> {
        let mut stmt = self.conn.prepare(
            r#"SELECT id, user_eth, basket_id, miden_recipient, amount_usdc,
                      status, requested_at_unix, last_event_unix,
                      claim_tx, bridge_tx, miden_consume_tx, erc20_mint_tx,
                      confirm_tx, refund_tx, basket_amount_minted, failure_reason
               FROM deposits WHERE id = ?1"#,
        )?;
        let mut rows = stmt.query(params![id as i64])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_record(row)?)),
            None => Ok(None),
        }
    }

    /// Returns every deposit that is not yet in a terminal state. The
    /// resume loop on relay startup walks this set.
    pub fn list_open(&self) -> Result<Vec<DepositRecord>> {
        let mut stmt = self.conn.prepare(
            r#"SELECT id, user_eth, basket_id, miden_recipient, amount_usdc,
                      status, requested_at_unix, last_event_unix,
                      claim_tx, bridge_tx, miden_consume_tx, erc20_mint_tx,
                      confirm_tx, refund_tx, basket_amount_minted, failure_reason
               FROM deposits
               WHERE status NOT IN ('settled', 'refunded', 'cancelled')
               ORDER BY id"#,
        )?;
        let rows = stmt
            .query_map([], |row| {
                row_to_record(row).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
                    )
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TxColumn {
    Claim,
    Bridge,
    MidenConsume,
    Erc20Mint,
    Confirm,
    Refund,
}

fn row_to_record(row: &rusqlite::Row<'_>) -> Result<DepositRecord> {
    let status_str: String = row.get(5)?;
    let status = parse_status(&status_str)?;
    let amount_str: String = row.get(4)?;
    let amount_usdc: u128 = amount_str.parse().context("amount_usdc parse")?;
    let basket_amount_str: Option<String> = row.get(14)?;
    let basket_amount_minted = basket_amount_str
        .map(|s| s.parse::<u128>())
        .transpose()
        .context("basket_amount_minted parse")?;
    Ok(DepositRecord {
        id: row.get::<_, i64>(0)? as u64,
        user_eth: row.get(1)?,
        basket_id: row.get(2)?,
        miden_recipient: row.get(3)?,
        amount_usdc,
        status,
        requested_at_unix: row.get(6)?,
        last_event_unix: row.get(7)?,
        claim_tx: row.get(8)?,
        bridge_tx: row.get(9)?,
        miden_consume_tx: row.get(10)?,
        erc20_mint_tx: row.get(11)?,
        confirm_tx: row.get(12)?,
        refund_tx: row.get(13)?,
        basket_amount_minted,
        failure_reason: row.get(15)?,
    })
}

fn parse_status(s: &str) -> Result<DepositStatus> {
    Ok(match s {
        "requested" => DepositStatus::Requested,
        "claimed" => DepositStatus::Claimed,
        "bridge_in_flight" => DepositStatus::BridgeInFlight,
        "bridged_to_miden" => DepositStatus::BridgedToMiden,
        "miden_minted" => DepositStatus::MidenMinted,
        "erc20_minted" => DepositStatus::Erc20Minted,
        "settled" => DepositStatus::Settled,
        "refunded" => DepositStatus::Refunded,
        "cancelled" => DepositStatus::Cancelled,
        other => return Err(anyhow!("unknown deposit status: {other}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(id: u64) -> DepositRecord {
        DepositRecord::new(
            id,
            format!("0xuser{id}"),
            "0xbasket".into(),
            "0xrecipient".into(),
            1_000_000,
            1_700_000_000,
        )
    }

    #[test]
    fn insert_and_get_round_trip() {
        let s = DepositStore::open_in_memory().unwrap();
        let r = sample(7);
        s.insert(&r).unwrap();
        let got = s.get(7).unwrap().unwrap();
        assert_eq!(got.id, 7);
        assert_eq!(got.user_eth, "0xuser7");
        assert_eq!(got.amount_usdc, 1_000_000);
        assert_eq!(got.status, DepositStatus::Requested);
    }

    #[test]
    fn update_status_persists() {
        let s = DepositStore::open_in_memory().unwrap();
        s.insert(&sample(1)).unwrap();
        s.update_status(1, DepositStatus::Claimed, 1_700_000_100).unwrap();
        let got = s.get(1).unwrap().unwrap();
        assert_eq!(got.status, DepositStatus::Claimed);
        assert_eq!(got.last_event_unix, 1_700_000_100);
    }

    #[test]
    fn set_tx_persists_per_column() {
        let s = DepositStore::open_in_memory().unwrap();
        s.insert(&sample(1)).unwrap();
        s.set_tx(1, TxColumn::Bridge, "0xbridgetxhash", 1_700_000_300).unwrap();
        let got = s.get(1).unwrap().unwrap();
        assert_eq!(got.bridge_tx.as_deref(), Some("0xbridgetxhash"));
    }

    #[test]
    fn list_open_excludes_terminal_states() {
        let s = DepositStore::open_in_memory().unwrap();
        s.insert(&sample(1)).unwrap();
        s.insert(&sample(2)).unwrap();
        s.insert(&sample(3)).unwrap();
        s.update_status(1, DepositStatus::Settled, 1_700_000_400).unwrap();
        s.update_status(2, DepositStatus::Refunded, 1_700_000_500).unwrap();
        s.update_status(3, DepositStatus::Claimed, 1_700_000_600).unwrap();
        let open = s.list_open().unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, 3);
    }

    #[test]
    fn update_status_on_unknown_errors() {
        let s = DepositStore::open_in_memory().unwrap();
        let err = s.update_status(999, DepositStatus::Claimed, 0).unwrap_err();
        assert!(err.to_string().contains("999"));
    }

    #[test]
    fn parse_status_round_trips() {
        for s in [
            DepositStatus::Requested,
            DepositStatus::Claimed,
            DepositStatus::BridgeInFlight,
            DepositStatus::BridgedToMiden,
            DepositStatus::MidenMinted,
            DepositStatus::Erc20Minted,
            DepositStatus::Settled,
            DepositStatus::Refunded,
            DepositStatus::Cancelled,
        ] {
            assert_eq!(parse_status(s.as_str()).unwrap(), s);
        }
    }
}
