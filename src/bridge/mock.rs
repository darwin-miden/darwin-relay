//! In-memory bridge mock. Pretends to perform an AggLayer
//! `bridgeAsset` and credit the Miden relay wallet. Used in tests and
//! in local dev until the canonical AggLayer bridge is publicly
//! available on Miden testnet.
//!
//! Behaviour: every transfer is `Confirmed` after a configurable delay
//! (default 0 — instant). The mock records every transfer so tests can
//! assert against it.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

use super::{BridgeClient, BridgeError, BridgeReceipt, BridgeStatus};

#[derive(Debug)]
struct InternalEntry {
    deposit_id: u64,
    amount_usdc: u128,
    status: BridgeStatus,
}

#[derive(Debug, Default)]
pub struct MockBridge {
    inner: Mutex<MockInner>,
    confirmation_delay: Duration,
    fail_on_amount: Option<u128>,
}

#[derive(Debug, Default)]
struct MockInner {
    next_ref: u64,
    entries: HashMap<String, InternalEntry>,
}

impl MockBridge {
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a mock that simulates an `n`-second bridge latency
    /// before flipping `Pending → Confirmed`.
    pub fn with_delay(secs: u64) -> Self {
        Self {
            confirmation_delay: Duration::from_secs(secs),
            ..Self::default()
        }
    }

    /// Make `bridge_out` deterministically fail when called with this
    /// exact amount. Useful for testing the relay's refund path.
    pub fn fail_on_amount(mut self, amount: u128) -> Self {
        self.fail_on_amount = Some(amount);
        self
    }

    /// Number of transfers the mock has seen.
    pub fn transfer_count(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    /// Snapshot the current state — useful for test assertions.
    pub fn snapshot(&self) -> Vec<(String, u64, u128, BridgeStatus)> {
        let inner = self.inner.lock().unwrap();
        let mut out: Vec<_> = inner
            .entries
            .iter()
            .map(|(r, e)| (r.clone(), e.deposit_id, e.amount_usdc, e.status))
            .collect();
        out.sort_by_key(|(_, d, _, _)| *d);
        out
    }
}

#[async_trait]
impl BridgeClient for MockBridge {
    async fn bridge_out(
        &self,
        deposit_id: u64,
        amount_usdc: u128,
    ) -> Result<BridgeReceipt, BridgeError> {
        if Some(amount_usdc) == self.fail_on_amount {
            return Err(BridgeError::Permanent(format!(
                "fail_on_amount triggered for deposit {deposit_id}"
            )));
        }
        let mut inner = self.inner.lock().unwrap();
        inner.next_ref += 1;
        let bridge_ref = format!("mock-bridge-{}", inner.next_ref);
        let status = if self.confirmation_delay.is_zero() {
            BridgeStatus::Confirmed
        } else {
            BridgeStatus::Pending
        };
        inner.entries.insert(
            bridge_ref.clone(),
            InternalEntry {
                deposit_id,
                amount_usdc,
                status,
            },
        );
        Ok(BridgeReceipt { bridge_ref, status })
    }

    async fn poll(&self, bridge_ref: &str) -> Result<BridgeReceipt, BridgeError> {
        let mut inner = self.inner.lock().unwrap();
        let entry = inner
            .entries
            .get_mut(bridge_ref)
            .ok_or_else(|| BridgeError::Permanent(format!("unknown bridge_ref {bridge_ref}")))?;
        // For the demo, pending entries flip to confirmed on the next
        // poll regardless of wall time — the relay loop is what enforces
        // the real cadence.
        if entry.status == BridgeStatus::Pending {
            entry.status = BridgeStatus::Confirmed;
        }
        Ok(BridgeReceipt {
            bridge_ref: bridge_ref.into(),
            status: entry.status,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn instant_mock_returns_confirmed() {
        let b = MockBridge::new();
        let r = b.bridge_out(42, 1_000_000).await.unwrap();
        assert_eq!(r.status, BridgeStatus::Confirmed);
        assert!(r.bridge_ref.starts_with("mock-bridge-"));
    }

    #[tokio::test]
    async fn delayed_mock_returns_pending_then_confirmed_on_poll() {
        let b = MockBridge::with_delay(5);
        let r = b.bridge_out(7, 500).await.unwrap();
        assert_eq!(r.status, BridgeStatus::Pending);
        let r2 = b.poll(&r.bridge_ref).await.unwrap();
        assert_eq!(r2.status, BridgeStatus::Confirmed);
    }

    #[tokio::test]
    async fn fail_on_amount_returns_permanent_error() {
        let b = MockBridge::new().fail_on_amount(666);
        let err = b.bridge_out(1, 666).await.unwrap_err();
        assert!(matches!(err, BridgeError::Permanent(_)));
    }

    #[tokio::test]
    async fn snapshot_orders_by_deposit_id() {
        let b = MockBridge::new();
        b.bridge_out(2, 100).await.unwrap();
        b.bridge_out(1, 200).await.unwrap();
        b.bridge_out(3, 300).await.unwrap();
        let snap = b.snapshot();
        assert_eq!(snap[0].1, 1);
        assert_eq!(snap[1].1, 2);
        assert_eq!(snap[2].1, 3);
    }

    #[tokio::test]
    async fn poll_unknown_ref_errors() {
        let b = MockBridge::new();
        let err = b.poll("nonexistent").await.unwrap_err();
        assert!(matches!(err, BridgeError::Permanent(_)));
    }
}
