//! In-memory `MidenSubmitter` for unit tests. Returns deterministic
//! fake tx hashes and applies the same 30 bps mint fee the v4
//! controller would apply on the real path (so the basket-amount
//! arithmetic flowing through the FSM matches production).
//!
//! Records every submission so tests can assert against them.

use std::sync::Mutex;

use async_trait::async_trait;

use super::{MidenError, MidenSubmitOutcome, MidenSubmitter};

const MINT_FEE_BPS: u128 = 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockMidenCall {
    pub deposit_id: u64,
    pub basket_id: String,
    pub amount_usdc: u128,
}

#[derive(Default, Debug)]
pub struct MockMidenSubmitter {
    inner: Mutex<MockInner>,
    fail_on_basket: Option<String>,
}

#[derive(Default, Debug)]
struct MockInner {
    counter: u64,
    calls: Vec<MockMidenCall>,
}

impl MockMidenSubmitter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Make `submit_deposit` permanently fail for this basket id.
    /// Used to exercise the route-to-refund path.
    pub fn fail_on_basket(mut self, basket_id: impl Into<String>) -> Self {
        self.fail_on_basket = Some(basket_id.into());
        self
    }

    pub fn calls(&self) -> Vec<MockMidenCall> {
        self.inner.lock().unwrap().calls.clone()
    }
}

#[async_trait]
impl MidenSubmitter for MockMidenSubmitter {
    async fn submit_deposit(
        &self,
        deposit_id: u64,
        basket_id: &str,
        amount_usdc: u128,
    ) -> Result<MidenSubmitOutcome, MidenError> {
        if self.fail_on_basket.as_deref() == Some(basket_id) {
            return Err(MidenError::Permanent(format!(
                "fail_on_basket triggered for {basket_id}"
            )));
        }
        let mut inner = self.inner.lock().unwrap();
        inner.counter += 1;
        inner.calls.push(MockMidenCall {
            deposit_id,
            basket_id: basket_id.into(),
            amount_usdc,
        });
        let consume_tx = format!("0xmockmidentx{:08}", inner.counter);
        let net = amount_usdc * (10_000 - MINT_FEE_BPS) / 10_000;
        Ok(MidenSubmitOutcome {
            consume_tx,
            basket_amount_minted: net,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn submit_returns_pro_rata_basket_amount() {
        let s = MockMidenSubmitter::new();
        let o = s.submit_deposit(1, "0xdcc", 1_000_000).await.unwrap();
        // 30 bps fee → 997_000 net
        assert_eq!(o.basket_amount_minted, 997_000);
        assert!(o.consume_tx.starts_with("0xmockmidentx"));
    }

    #[tokio::test]
    async fn records_each_call() {
        let s = MockMidenSubmitter::new();
        s.submit_deposit(1, "0xdcc", 100).await.unwrap();
        s.submit_deposit(2, "0xdag", 200).await.unwrap();
        let calls = s.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].basket_id, "0xdcc");
        assert_eq!(calls[1].basket_id, "0xdag");
    }

    #[tokio::test]
    async fn fail_on_basket_returns_permanent() {
        let s = MockMidenSubmitter::new().fail_on_basket("0xunknown");
        let err = s.submit_deposit(1, "0xunknown", 1_000).await.unwrap_err();
        assert!(matches!(err, MidenError::Permanent(_)));
        // other baskets still succeed
        s.submit_deposit(2, "0xdcc", 1_000).await.unwrap();
    }

    #[tokio::test]
    async fn consume_tx_hashes_are_monotonic() {
        let s = MockMidenSubmitter::new();
        let a = s.submit_deposit(1, "0xa", 1).await.unwrap();
        let b = s.submit_deposit(2, "0xb", 1).await.unwrap();
        assert_ne!(a.consume_tx, b.consume_tx);
    }
}
