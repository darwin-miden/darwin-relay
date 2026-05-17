//! Darwin Relay — bridges ETH-side deposits into Miden-side private
//! basket positions, mints a wrapped ERC20 on ETH so the user never
//! has to touch Miden.
//!
//! Architecture (see darwin-relay/docs/architecture.md):
//!
//! ```text
//!   ETH user
//!     │ approve + deposit(USDC, basketId, midenRecipient)
//!     ▼
//!   DarwinRelayDeposit.sol  ── RelayDepositRequested ──►  darwin-relay
//!     │                                                       │
//!     │                                                       │ bridge USDC
//!     │                                                       │ AggLayer or Mock
//!     │                                                       ▼
//!     │                                              Relay wallet on Miden
//!     │                                                       │ build DepositNote
//!     │                                                       ▼
//!     │                                              v4 controller consumes
//!     │                                                       │ basket position minted (private)
//!     │                                                       │
//!     │ confirmDeposit(id, amount)                            │
//!     │◄──────────────────────────────────────────────────────┘
//!     │
//!     ▼
//!   DarwinBasketToken.mintTo(user, amount)
//! ```
//!
//! Crate layout:
//!
//! - [`state`]: deposit state machine + persisted record types
//! - [`bridge`]: `BridgeClient` trait + Mock + AggLayer implementations
//! - [`store`]: SQLite-backed deposit persistence
//! - [`service`]: tokio orchestrator that wires it all together

pub mod bridge;
pub mod eth;
pub mod service;
pub mod state;
pub mod store;
pub mod watcher;
