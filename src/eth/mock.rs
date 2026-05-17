//! In-memory `EthClient` for unit tests. Records every call as a
//! `MockCall` so tests can assert against them. Returns deterministic
//! fake tx hashes so the FSM has stable data to persist.

use std::sync::Mutex;

use async_trait::async_trait;

use super::{EthClient, EthError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MockCall {
    Claim(u64),
    Confirm { id: u64, basket_amount: u128 },
    Refund { id: u64, reason: String },
    MintTo { basket_id: String, user: String, amount: u128 },
}

#[derive(Default, Debug)]
pub struct MockEthClient {
    inner: Mutex<MockInner>,
    fail_on_claim: Option<u64>,
}

#[derive(Default, Debug)]
struct MockInner {
    counter: u64,
    calls: Vec<MockCall>,
}

impl MockEthClient {
    pub fn new() -> Self {
        Self::default()
    }

    /// Make `claim_deposit(id)` fail permanently when called with
    /// this exact id. Used to exercise the refund-on-failure path.
    pub fn fail_on_claim(mut self, id: u64) -> Self {
        self.fail_on_claim = Some(id);
        self
    }

    pub fn calls(&self) -> Vec<MockCall> {
        self.inner.lock().unwrap().calls.clone()
    }

    pub fn call_count(&self) -> usize {
        self.inner.lock().unwrap().calls.len()
    }

    fn record(&self, call: MockCall) -> String {
        let mut inner = self.inner.lock().unwrap();
        inner.counter += 1;
        let hash = format!("0xmocktx{:08}", inner.counter);
        inner.calls.push(call);
        hash
    }
}

#[async_trait]
impl EthClient for MockEthClient {
    async fn claim_deposit(&self, deposit_id: u64) -> Result<String, EthError> {
        if Some(deposit_id) == self.fail_on_claim {
            return Err(EthError::Permanent(format!(
                "fail_on_claim triggered for {deposit_id}"
            )));
        }
        Ok(self.record(MockCall::Claim(deposit_id)))
    }

    async fn confirm_deposit(
        &self,
        deposit_id: u64,
        basket_amount: u128,
    ) -> Result<String, EthError> {
        Ok(self.record(MockCall::Confirm {
            id: deposit_id,
            basket_amount,
        }))
    }

    async fn refund_deposit(&self, deposit_id: u64, reason: &str) -> Result<String, EthError> {
        Ok(self.record(MockCall::Refund {
            id: deposit_id,
            reason: reason.into(),
        }))
    }

    async fn mint_basket_to(
        &self,
        basket_id: &str,
        user: &str,
        amount: u128,
    ) -> Result<String, EthError> {
        Ok(self.record(MockCall::MintTo {
            basket_id: basket_id.into(),
            user: user.into(),
            amount,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn records_each_call() {
        let c = MockEthClient::new();
        c.claim_deposit(1).await.unwrap();
        c.confirm_deposit(1, 1000).await.unwrap();
        c.mint_basket_to("0xdcc", "0xuser", 997).await.unwrap();
        let calls = c.calls();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0], MockCall::Claim(1));
        assert!(matches!(calls[1], MockCall::Confirm { id: 1, basket_amount: 1000 }));
    }

    #[tokio::test]
    async fn returns_monotonic_tx_hashes() {
        let c = MockEthClient::new();
        let h1 = c.claim_deposit(1).await.unwrap();
        let h2 = c.claim_deposit(2).await.unwrap();
        assert_ne!(h1, h2);
        assert!(h1.starts_with("0xmocktx"));
        assert!(h2.starts_with("0xmocktx"));
    }

    #[tokio::test]
    async fn fail_on_claim_returns_permanent_error() {
        let c = MockEthClient::new().fail_on_claim(42);
        let err = c.claim_deposit(42).await.unwrap_err();
        assert!(matches!(err, EthError::Permanent(_)));
        // other ids still succeed
        c.claim_deposit(7).await.unwrap();
    }

    #[tokio::test]
    async fn refund_call_captures_reason_string() {
        let c = MockEthClient::new();
        c.refund_deposit(9, "bridge dead").await.unwrap();
        let calls = c.calls();
        assert!(matches!(&calls[0], MockCall::Refund { id: 9, reason } if reason == "bridge dead"));
    }
}
