//! Real `DepositWatcher` over alloy 1.x's `subscribe_logs`. Connects
//! over WebSocket to a JSON-RPC URL, filters on
//! `RelayDepositRequested(uint256,address,bytes32,uint256,bytes32,uint64)`,
//! decodes each log into an `ObservedDeposit`, and yields them via
//! `next()`.
//!
//! Crash semantics: the watcher only emits new events. The driver's
//! resume loop is what picks up deposits the relay missed while
//! offline (it walks `DepositStore::list_open()` on startup, and a
//! lightweight backfill RPC call seeds any deposits not yet stored).
//! The backfill itself lands as a small follow-up; this scaffold
//! covers the live-stream half.

use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::Filter;
use alloy::sol;
use alloy::sol_types::SolEvent;
use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::mpsc;

use super::{DepositWatcher, ObservedDeposit, WatcherError};

sol! {
    event RelayDepositRequested(
        uint256 indexed id,
        address indexed user,
        bytes32 indexed basketId,
        uint256 amount,
        bytes32 midenRecipient,
        uint64  requestedAt
    );
}

pub struct AlloyWatcher {
    rx: mpsc::Receiver<ObservedDeposit>,
}

impl AlloyWatcher {
    /// Start a background tokio task that subscribes to logs on
    /// `relay_address` and forwards each decoded deposit to an
    /// internal channel. The returned `AlloyWatcher` is the consumer
    /// end (call `.next().await`).
    ///
    /// Requires a WebSocket URL (`ws://` or `wss://`). HTTP polling
    /// fallback can be added in a follow-up.
    pub async fn start(ws_url: &str, relay_address: Address) -> Result<Self, WatcherError> {
        let provider = ProviderBuilder::new()
            .connect_ws(alloy::providers::WsConnect::new(ws_url.to_string()))
            .await
            .map_err(|e| WatcherError::Transient(format!("ws connect: {e}")))?;

        let topic0 = RelayDepositRequested::SIGNATURE_HASH;
        let filter = Filter::new()
            .address(relay_address)
            .event_signature(topic0);

        let sub = provider
            .subscribe_logs(&filter)
            .await
            .map_err(|e| WatcherError::Transient(format!("subscribe_logs: {e}")))?;
        let mut stream = sub.into_stream();

        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            while let Some(log) = stream.next().await {
                let observed = match decode_log(&log) {
                    Ok(o) => o,
                    Err(e) => {
                        tracing::warn!(error = %e, "skip undecodable log");
                        continue;
                    }
                };
                if tx.send(observed).await.is_err() {
                    break;
                }
            }
        });
        Ok(Self { rx })
    }
}

#[async_trait]
impl DepositWatcher for AlloyWatcher {
    async fn next(&mut self) -> Result<ObservedDeposit, WatcherError> {
        self.rx.recv().await.ok_or(WatcherError::Closed)
    }
}

fn decode_log(log: &alloy::rpc::types::Log) -> Result<ObservedDeposit, String> {
    let decoded = RelayDepositRequested::decode_log(&log.inner)
        .map_err(|e| format!("decode_log: {e}"))?;
    let id: u64 = decoded
        .id
        .try_into()
        .map_err(|_| "id > u64::MAX".to_string())?;
    let amount_usdc: u128 = u256_to_u128(decoded.amount)?;
    Ok(ObservedDeposit {
        id,
        user_eth: format!("0x{}", hex::encode(decoded.user.as_slice())),
        basket_id: format!("0x{}", hex::encode(decoded.basketId.as_slice())),
        miden_recipient: format!("0x{}", hex::encode(decoded.midenRecipient.as_slice())),
        amount_usdc,
        requested_at_unix: decoded.requestedAt as i64,
    })
}

fn u256_to_u128(v: U256) -> Result<u128, String> {
    if v.bit_len() > 128 {
        return Err(format!("amount {v} > u128::MAX"));
    }
    let bytes = v.to_be_bytes::<32>();
    Ok(u128::from_be_bytes(bytes[16..].try_into().unwrap()))
}

// Silence "unused" warnings on the helpers that are private to this
// module but only exercised by the decode path.
#[allow(dead_code)]
fn _unused_b256_marker(_b: B256, _a: Address, _s: String) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u256_to_u128_round_trip() {
        let v = U256::from(1_000_000u128);
        assert_eq!(u256_to_u128(v).unwrap(), 1_000_000u128);
    }

    #[test]
    fn u256_to_u128_overflow_errors() {
        let v = U256::MAX;
        assert!(u256_to_u128(v).is_err());
    }

    #[test]
    fn address_str_parses() {
        use std::str::FromStr;
        let a = Address::from_str("0x0000000000000000000000000000000000000001").unwrap();
        assert_eq!(format!("0x{}", hex::encode(a.as_slice())).len(), 2 + 40);
    }
}
