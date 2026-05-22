# darwin-relay

A Miden-side custodial wallet that lets an ETH-native user deposit
ETH and end up holding a Darwin basket position natively on Miden,
without ever managing a Miden key.

Two binaries live in this repo:

| Binary | Path | Role | Default port |
|---|---|---|---|
| `darwin_relay_v2` | `src/bin/darwin_relay_v2.rs` | **Current.** Miden-side custodial wallet brokered by NEAR Intents 1Click. The relay is the 1Click bridge recipient and holds the basket-token position on the user's behalf, keyed by their EVM address. | `0.0.0.0:8090` |
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

GET  /v0/positions/:evm_addr   → list of relay-held basket positions
GET  /health
```

## v2 — state machine

```
deposit:  QUOTED → KNOWN_DEPOSIT_TX → PROCESSING → ONECLICK_SUCCESS → POSITION_CREDITED
redeem:   REQUESTED → SETTLED
```

A background poller hits the 1Click bridge's `/v0/status` for active
intents every `DARWIN_RELAY_V2_POLL_INTERVAL_S` seconds (default 10).
Once an intent reaches `POSITION_CREDITED` it leaves the active set
and is never credited twice.

The on-chain `atomic_deposit_note` submission against the basket
controller is deferred to an off-process miden-client worker (the
client's futures are `!Send` and would block axum's runtime).
The relay's accounting tracks the position immediately and the worker
reconciles `atomic_deposit_tx` + `miden_consume_tx` in place.

## v2 — running it

```bash
cargo build --release --bin darwin_relay_v2 --features v2
./target/release/darwin_relay_v2
```

Configuration via env (all optional, defaults shown):

```bash
DARWIN_RELAY_V2_BIND=0.0.0.0:8090
DARWIN_RELAY_V2_STORE=./relay-v2.sqlite
DARWIN_RELAY_V2_ONECLICK_URL=http://localhost:8080
DARWIN_RELAY_V2_RELAY_WALLET_HEX=0xed3cd5befa3207805f8529207cfc0d
DARWIN_RELAY_V2_CONTROLLER_HEX=0xa25aa0b00007688024b74b05a52aab
DARWIN_RELAY_V2_POLL_INTERVAL_S=10
```

## End-to-end verification

`scripts/oneclick_e2e.sh` walks the full deposit + redeem cycle
against a local 1Click mock (Brian's `miden-testnet-bridge` Sepolia
profile) and the v2 binary. It expects:

- `darwin_relay_v2` running on `:8090`
- A 1Click mock running on `:8080`
- A funded Sepolia EOA exported as `DARWIN_DEV_KEY`
- `cast` on `$PATH` (foundry)

Run it with `bash scripts/oneclick_e2e.sh` once both services are up.

## v1 — historical

The v1 ETH-side escrow contract `DarwinRelayDeposit.sol` and the
v1 binary `darwin_relay_service` are preserved for reference. See
the git history before commit `e620dd6` for the v1 architecture
notes that previously occupied this file.

## License

MIT
