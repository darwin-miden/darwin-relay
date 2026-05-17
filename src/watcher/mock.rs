//! In-memory `DepositWatcher` backed by a tokio mpsc channel. Tests
//! push synthetic `ObservedDeposit`s in, the watcher pops them in
//! arrival order.

use tokio::sync::mpsc;

use async_trait::async_trait;

use super::{DepositWatcher, ObservedDeposit, WatcherError};

pub struct MockWatcher {
    rx: mpsc::Receiver<ObservedDeposit>,
}

impl MockWatcher {
    pub fn new(buffer: usize) -> (Self, MockWatcherHandle) {
        let (tx, rx) = mpsc::channel(buffer);
        (Self { rx }, MockWatcherHandle { tx })
    }
}

#[derive(Clone)]
pub struct MockWatcherHandle {
    tx: mpsc::Sender<ObservedDeposit>,
}

impl MockWatcherHandle {
    pub async fn push(&self, d: ObservedDeposit) {
        self.tx.send(d).await.expect("MockWatcher rx dropped");
    }

    pub fn close(self) {
        drop(self.tx);
    }
}

#[async_trait]
impl DepositWatcher for MockWatcher {
    async fn next(&mut self) -> Result<ObservedDeposit, WatcherError> {
        self.rx.recv().await.ok_or(WatcherError::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn push_and_next_round_trip() {
        let (mut w, h) = MockWatcher::new(16);
        let dep = ObservedDeposit {
            id: 1,
            user_eth: "0xuser".into(),
            basket_id: "0xdcc".into(),
            miden_recipient: "0x0".into(),
            amount_usdc: 1_000_000,
            requested_at_unix: 1_700_000_000,
        };
        h.push(dep.clone()).await;
        let got = w.next().await.unwrap();
        assert_eq!(got, dep);
    }

    #[tokio::test]
    async fn close_propagates_to_next() {
        let (mut w, h) = MockWatcher::new(1);
        h.close();
        let err = w.next().await.unwrap_err();
        assert!(matches!(err, WatcherError::Closed));
    }

    #[tokio::test]
    async fn deposits_arrive_in_send_order() {
        let (mut w, h) = MockWatcher::new(8);
        for id in 1..=3 {
            h.push(ObservedDeposit {
                id,
                user_eth: "0xu".into(),
                basket_id: "0xb".into(),
                miden_recipient: "0x0".into(),
                amount_usdc: 100,
                requested_at_unix: 0,
            })
            .await;
        }
        assert_eq!(w.next().await.unwrap().id, 1);
        assert_eq!(w.next().await.unwrap().id, 2);
        assert_eq!(w.next().await.unwrap().id, 3);
    }
}
