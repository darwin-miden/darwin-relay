//! Deposit state machine — mirrors the `DarwinRelayDeposit.sol` Status
//! enum + adds off-chain steps the contract doesn't see (BridgeInFlight,
//! BridgedToMiden, MidenMinted).
//!
//! Transitions (happy path):
//!
//!   Requested → Claimed → BridgeInFlight → BridgedToMiden
//!                                              ↓
//!                                          MidenMinted → Erc20Minted → Settled
//!
//! Failure paths terminate in Refunded (relay-driven) or Cancelled
//! (user-driven).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DepositStatus {
    /// User has deposited; relay hasn't picked it up yet.
    Requested,
    /// Relay has called `claimDeposit` on the contract. State is locked
    /// from user-cancel until the relay reaches a terminal state.
    Claimed,
    /// Relay submitted the bridge tx (or its mock equivalent).
    BridgeInFlight,
    /// Bridge confirmed: relay wallet on Miden holds the bridged asset.
    BridgedToMiden,
    /// Relay submitted the DepositNote and the v4 controller consumed
    /// it. Basket position now lives in the controller's private vault.
    MidenMinted,
    /// Relay called `DarwinBasketToken.mintTo(user, amount)` on ETH.
    /// User now holds wrapped ERC20 in their wallet.
    Erc20Minted,
    /// Relay called `confirmDeposit` on the escrow. Escrowed USDC moved
    /// to the operator treasury. Terminal.
    Settled,
    /// Relay called `refundDeposit` on the escrow. User got USDC back.
    /// Terminal.
    Refunded,
    /// User called `cancelDeposit` after claim window elapsed. Terminal.
    Cancelled,
}

impl DepositStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Settled | Self::Refunded | Self::Cancelled)
    }

    /// Returns the next status in the happy path, or None if terminal /
    /// awaiting external event.
    pub fn happy_next(self) -> Option<Self> {
        Some(match self {
            Self::Requested => Self::Claimed,
            Self::Claimed => Self::BridgeInFlight,
            Self::BridgeInFlight => Self::BridgedToMiden,
            Self::BridgedToMiden => Self::MidenMinted,
            Self::MidenMinted => Self::Erc20Minted,
            Self::Erc20Minted => Self::Settled,
            Self::Settled | Self::Refunded | Self::Cancelled => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Claimed => "claimed",
            Self::BridgeInFlight => "bridge_in_flight",
            Self::BridgedToMiden => "bridged_to_miden",
            Self::MidenMinted => "miden_minted",
            Self::Erc20Minted => "erc20_minted",
            Self::Settled => "settled",
            Self::Refunded => "refunded",
            Self::Cancelled => "cancelled",
        }
    }
}

/// One row in the relay's deposit table. Mirrors enough of the
/// on-chain `Deposit` struct that we can recover state if the relay
/// process restarts mid-flight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositRecord {
    pub id: u64,
    pub user_eth: String,            // 0x… address
    pub basket_id: String,           // 0x… hex of bytes32 keccak256(symbol)
    pub miden_recipient: String,     // 0x… hex of bytes32 (zero if user has no Miden account)
    pub amount_usdc: u128,
    pub status: DepositStatus,
    pub requested_at_unix: i64,
    pub last_event_unix: i64,
    /// Tx hashes accumulated as the deposit progresses through stages.
    pub claim_tx: Option<String>,
    pub bridge_tx: Option<String>,
    pub miden_consume_tx: Option<String>,
    pub erc20_mint_tx: Option<String>,
    pub confirm_tx: Option<String>,
    pub refund_tx: Option<String>,
    pub basket_amount_minted: Option<u128>,
    pub failure_reason: Option<String>,
}

impl DepositRecord {
    pub fn new(
        id: u64,
        user_eth: String,
        basket_id: String,
        miden_recipient: String,
        amount_usdc: u128,
        requested_at_unix: i64,
    ) -> Self {
        Self {
            id,
            user_eth,
            basket_id,
            miden_recipient,
            amount_usdc,
            status: DepositStatus::Requested,
            requested_at_unix,
            last_event_unix: requested_at_unix,
            claim_tx: None,
            bridge_tx: None,
            miden_consume_tx: None,
            erc20_mint_tx: None,
            confirm_tx: None,
            refund_tx: None,
            basket_amount_minted: None,
            failure_reason: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_walks_all_states() {
        let mut s = DepositStatus::Requested;
        let expected = [
            DepositStatus::Claimed,
            DepositStatus::BridgeInFlight,
            DepositStatus::BridgedToMiden,
            DepositStatus::MidenMinted,
            DepositStatus::Erc20Minted,
            DepositStatus::Settled,
        ];
        for want in expected {
            s = s.happy_next().expect("non-terminal");
            assert_eq!(s, want);
        }
        assert!(s.is_terminal());
        assert!(s.happy_next().is_none());
    }

    #[test]
    fn terminal_states_have_no_next() {
        for t in [
            DepositStatus::Settled,
            DepositStatus::Refunded,
            DepositStatus::Cancelled,
        ] {
            assert!(t.is_terminal());
            assert!(t.happy_next().is_none());
        }
    }

    #[test]
    fn status_as_str_is_stable_snake_case() {
        assert_eq!(DepositStatus::BridgeInFlight.as_str(), "bridge_in_flight");
        assert_eq!(DepositStatus::Erc20Minted.as_str(), "erc20_minted");
    }

    #[test]
    fn deposit_record_initial_state() {
        let r = DepositRecord::new(
            1,
            "0xabc".into(),
            "0xdef".into(),
            "0x0".into(),
            1_000_000,
            1_700_000_000,
        );
        assert_eq!(r.status, DepositStatus::Requested);
        assert!(r.claim_tx.is_none());
        assert!(r.bridge_tx.is_none());
        assert_eq!(r.last_event_unix, r.requested_at_unix);
    }
}
