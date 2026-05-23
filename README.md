# darwin-relay

A Miden-side custodial wallet that lets an ETH-native user deposit
ETH and end up holding a Darwin basket position natively on Miden,
without ever managing a Miden key.

Three binaries live in this repo:

| Binary | Path | Role | Default port |
|---|---|---|---|
| `darwin_relay_v2` | `src/bin/darwin_relay_v2.rs` | **Current.** Axum REST service. Holds off-chain accounting for ETH-user intents + relay-held basket positions, brokers 1Click on the Sepolia side. | `0.0.0.0:8090` |
| `darwin_relay_v2_worker` | `src/bin/darwin_relay_v2_worker.rs` | **On-chain leg.** Reads the same SQLite store, drives `atomic_deposit_note`, `atomic_redeem_note`, and `bridge_out_v1` submissions on Miden; polls 1Click for outbound settlement. Runs in its own process (tokio current_thread) because the miden-client futures are `!Send`. | n/a |
| `darwin_relay_service` | `src/bin/darwin_relay_service.rs` | Historical. Sepolia escrow + wDCC wrapped-ERC20 path from M2. Kept buildable for reference; no longer wired into the frontend. | n/a |

The v2 design is described in [`SPEC-v2.md`](SPEC-v2.md). Why the
reshape: the original v1 collapsed three roles (bridge, relay wallet,
AggLayer) into a single Sepolia escrow + a wrapped ERC20. With
BrianSeong99/miden-testnet-bridge (a NEAR Intents 1Click mock for
Miden) and Bali AggLayer reachable, the proposal's native shape is
implementable — the bridge is delegated to 1Click and `darwin-relay`
collapses back to just the Miden-side custodial layer.

## v2 — REST surface

```
POST /v0/intents               { user_evm_addr, basket_symbol, amount_in_wei }
                                 → { correlation_id, relay_miden_address, expires_at }

GET  /v0/intents/:id           → full intent record
POST /v0/intents/:id/deposit   { deposit_address, sepolia_tx }
                                 → { ok, stage }

POST /v0/redeem                { user_evm_addr, basket_symbol, basket_amount }
                                 → { redemption_id, stage }
GET  /v0/redeem/:id            → full redemption record
GET  /v0/redemptions/:evm_addr → list of user's redemptions (UI hook)

GET  /v0/positions/:evm_addr   → list of relay-held basket positions
GET  /health
```

## v2 — state machine

```
deposit:  QUOTED → KNOWN_DEPOSIT_TX → PROCESSING → ONECLICK_SUCCESS → POSITION_CREDITED
                                                                       │
                                                                       ▼ (worker)
                                                                  atomic_deposit_tx
                                                                  miden_consume_tx

redeem:   REQUESTED → SETTLED  (off-chain debit, immediate)
                       │
                       ▼ (worker)
                  miden_redeem_tx           (atomic_redeem_note submitted on Miden)
                       │
                       ▼ (worker, outbound)
                  miden_bridge_out_tx       (bridge_out_v1 P2ID emitted to 1Click bridge account)
                       │
                       ▼ (worker, status poll)
                  sepolia_release_tx        + stage = FULLY_SETTLED
```

The axum service drives the off-chain leg only (debits/credits, 1Click
inbound polling). The on-chain leg lives in `darwin_relay_v2_worker`
because the miden-client futures are `!Send` and can't share axum's
runtime. Both processes read/write the same SQLite store at
`$DARWIN_RELAY_V2_STORE`.

## v2 — running it

```bash
# Axum REST service (off-chain accounting + 1Click inbound poll).
cargo build --release --bin darwin_relay_v2 --features v2
./target/release/darwin_relay_v2

# On-chain worker (atomic_deposit / atomic_redeem / bridge_out_v1).
cargo build --release --bin darwin_relay_v2_worker --features v2-worker
./target/release/darwin_relay_v2_worker
```

Both binaries read the same defaults; override via env:

```bash
# Shared by both
DARWIN_RELAY_V2_STORE=./relay-v2.sqlite
DARWIN_RELAY_V2_ONECLICK_URL=http://localhost:8080
DARWIN_RELAY_V2_RELAY_WALLET_HEX=0xed3cd5befa3207805f8529207cfc0d
DARWIN_RELAY_V2_CONTROLLER_HEX=0xa25aa0b00007688024b74b05a52aab

# Axum-only
DARWIN_RELAY_V2_BIND=0.0.0.0:8090
DARWIN_RELAY_V2_POLL_INTERVAL_S=10

# Worker-only
DARWIN_RELAY_V2_MIDEN_STORE=$HOME/.miden/store.sqlite3
DARWIN_RELAY_V2_MIDEN_KEYSTORE=$HOME/.miden/keystore
DARWIN_RELAY_V2_FAUCET_HEX=0xa095d9b3831e96206ff70c2218a6a9          # dETH (controller-recognised)
DARWIN_RELAY_V2_OUTBOUND_FAUCET_HEX=0x7aabde381e7ac6a06b22534a6900cb # 1Click eth (bridge-recognised)
DARWIN_RELAY_V2_WORKER_INTERVAL_S=30
```

## End-to-end verification

Two scripts in `scripts/`:

- `oneclick_e2e.sh` — drives a full deposit + half-redeem cycle through
  the axum service only (off-chain accounting). Fast smoke test for
  the v2 surface.
- `v2_worker_full.sh` — runs both binaries and exercises the full
  on-chain pipeline:
    1. POST /v0/intents on the relay
    2. 1Click /v0/quote → cast send Sepolia → dual-notify
    3. Wait for POSITION_CREDITED
    4. Wait for the worker to populate `atomic_deposit_tx`
    5. POST /v0/redeem
    6. Wait for the worker to populate `miden_redeem_tx`
    7. Wait for the worker to populate `miden_bridge_out_tx`
    8. Wait for 1Click → `sepolia_release_tx` + stage = FULLY_SETTLED

Both expect `darwin_relay_v2` on `:8090`, the 1Click mock on `:8080`,
`cast` on `$PATH`, and `DARWIN_DEV_KEY` exported. `v2_worker_full.sh`
additionally needs `darwin_relay_v2_worker` running in another shell.

## v1 — historical

The v1 ETH-side escrow contract `DarwinRelayDeposit.sol` and the
v1 binary `darwin_relay_service` are preserved for reference. See
the git history before commit `e620dd6` for the v1 architecture
notes that previously occupied this file.

## License

MIT
