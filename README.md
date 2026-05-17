# darwin-relay

ETH-side escrow plus a Miden-side operator wallet that lets an
ETH-native user deposit USDC and end up holding a wrapped Darwin
basket ERC20 on Ethereum, without ever touching Miden or managing a
Miden key.

## Why

The grant for Milestone 2 originally referenced Near Intents +
Miden Guardian as the canonical onboarding stack for ETH users. As
of 2026-05-17 Near Intents does not list Miden as a destination chain,
and Miden Guardian is a non-custodial state-coordinator (it never
moves funds for users). Until those land in their canonical forms,
Darwin operates its own minimal relay so the M2 flow ships.

## Architecture

```text
   ETH user
     │ approve + deposit(USDC, basketId, midenRecipient)
     ▼
   DarwinRelayDeposit.sol  ── RelayDepositRequested ──►  darwin-relay
     │                                                       │
     │                                                       │ bridge USDC
     │                                                       │ AggLayer or Mock
     │                                                       ▼
     │                                              Relay wallet on Miden
     │                                                       │ build DepositNote
     │                                                       ▼
     │                                              v4 controller consumes
     │                                                       │ basket position minted (private)
     │                                                       │
     │ confirmDeposit(id, basketAmountMinted)                │
     │◄──────────────────────────────────────────────────────┘
     ▼
   DarwinBasketToken.mintTo(user, basketAmount)
```

The user surface is a single ETH transaction (`deposit`). All the
Miden plumbing happens in the relay service, paid for by the deposit
itself.

## Components

| Path | What |
|---|---|
| `contracts/DarwinRelayDeposit.sol` | ETH escrow with claim/confirm/cancel/refund state machine |
| `contracts/test/DarwinRelayDeposit.t.sol` | 25 Foundry tests covering every transition |
| `src/state.rs` | Deposit FSM types: 9 states (Requested → Settled / Refunded / Cancelled) |
| `src/bridge/mod.rs` | `BridgeClient` trait + `MockBridge` impl |
| `src/store.rs` | SQLite-backed persistence + resume loop |
| `src/service.rs` | tokio orchestrator that drives each deposit through its FSM |
| `src/bin/darwin_relay_service.rs` | Smoke entry point (inserts a sample deposit + drives it) |

## State machine

```text
   Requested  ─claim→  Claimed  ─bridge→  BridgeInFlight  ─poll→  BridgedToMiden
                                                                       │
                                                                       │ depositNote
                                                                       ▼
                                                                  MidenMinted
                                                                       │
                                                                       │ erc20 mintTo
                                                                       ▼
                                                                  Erc20Minted
                                                                       │
                                                                       │ confirmDeposit
                                                                       ▼
                                                                    Settled
```

Failure transitions terminate in `Refunded` (relay-driven) or `Cancelled`
(user-driven after the claim window).

## Local dev

```bash
# ETH side
forge test           # 25 tests
forge build

# Rust side
cargo test           # 19 tests (state + bridge + store + service)
cargo run --bin darwin_relay_service   # smoke: drives a sample deposit to Settled
```

## Status (2026-05-17)

- ETH escrow contract: scaffold complete, 25/25 Foundry tests green
- Rust service: scaffold complete, 19/19 tests green
- Bridge: MockBridge only — real AggLayer integration lands when the
  canonical Miden ↔ Ethereum bridge is publicly live on testnet
- Miden side: stub tx hashes in the FSM — real DepositNote submission
  via miden-client lands as iteration 2 (gated behind the `miden-live`
  feature)
- ETH event watcher (alloy WS subscription on RelayDepositRequested):
  iteration 2

## License

MIT
