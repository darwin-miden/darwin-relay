# darwin-relay v2 — Miden-side custodial wallet for ETH users

Version: 0.1 (spec), 2026-05-22
Status: design — not yet built. Supersedes v1 (Sepolia escrow + wDCC ERC20).

## Why v2 exists

The grant proposal architecture for ETH users (Flow A, second path) reads:

> ETH users route through **Near Intent and a relay wallet**, bridging assets
> to Miden via AggLayer. In both cases, the Transaction Kernel validates
> the proof, Pragma Oracle provides token prices, and basket tokens are
> minted natively on Miden as a private STARK-proven note.

> ETH users receive assets via AggLayer BridgeAsset, **automatically
> withdrawn to their EVM wallet by the relay wallet**.

There are three components in that path:

1. **Near Intent** = the cross-chain bridge protocol (what BrianSeong99/
   miden-testnet-bridge mocks today).
2. **Relay wallet** = a custodial Miden account that holds the bridged
   asset on behalf of an ETH user (since the user has no Miden wallet),
   drives the Darwin atomic_deposit / atomic_redeem flow, holds the
   basket-token position, and ships underlyings back on redeem.
3. **AggLayer** = the canonical asset transport (Bali agglayer at
   gateway-fm; testable today, fully merged docs not yet).

v1 of `darwin-relay` was a workaround built during M2 when NEAR Intent
didn't list Miden as a supported destination. It implemented a Sepolia-
side escrow that minted a wrapped basket-token ERC20 (wDCC) back to the
user — assets never actually crossed to Miden. With Brian's mock 1Click
now live and the Bali agglayer reachable, the proposal's native shape is
achievable, and v1's wDCC ERC20 path becomes redundant.

v2 reshapes `darwin-relay` to be component (2) only — the Miden-side
custodial layer — and delegates the bridge transport to 1Click / AggLayer.

## Surface

A single long-running Rust binary, `darwin-relay-v2`, that:

```
listens for inbound 1Click P2ID notes targeted at the relay wallet
  -> consumes the note (asset arrives in the relay wallet's vault)
  -> looks up the originating correlationId in 1Click status
  -> submits atomic_deposit_note targeting the basket controller
  -> records {correlationId, user_evm_addr, basket_symbol, basket_amount}
     in the local store as the user's open position

accepts /v0/redeem requests (REST) referring to a stored position
  -> submits atomic_redeem_note for that basket-token amount
  -> consumes the controller's payout notes for each constituent
  -> POSTs an outbound 1Click quote (Miden -> Sepolia native ETH)
  -> creates the corresponding Miden BridgeOutV1 note
  -> reports completion (Sepolia release tx hash) back to the requester
```

## Components

### relay-wallet (Miden account)

- One Miden account, deterministic from a master seed in `MIDEN_MASTER_SEED_HEX`.
- Falcon-512 single-sig (private storage).
- Storage slot 0..3: standard auth slots.
- Storage slot 4: positions map (user_evm_addr -> basket_token_balance,
  basket_symbol). Read-only off-chain; the relay binary owns writes.
- Guardian multisig wraps the master seed at-rest (per proposal: Guardian
  = state backup + multisig). The relay binary signs txs from a hot
  derivation; Guardian holds the cold key for recovery.

### oneclick-listener

Subscribes to the 1Click bridge's `/v0/status` polling endpoint, watching
for SUCCESS deposits targeted at our relay wallet's Miden id. On a hit:

1. Sync miden-client.
2. Find the inbound P2ID note (faucet = 1Click solver, recipient = relay).
3. Consume the note (asset lands in relay vault).
4. Read the quote metadata to recover the `correlationId` and the
   originating EVM address (`refundTo` in the 1Click quote).
5. Look up the basket symbol from the local correlationId mapping.
6. Submit atomic_deposit_note for that basket via the existing flow.
7. Persist the resulting position in storage slot 4.

### outbound-handler (REST)

POST `/v0/redeem` with body `{ user_evm_addr, basket_symbol, basket_amount }`:

1. Validate the user has at least `basket_amount` of `basket_symbol` in
   the relay's positions map.
2. Submit atomic_redeem_note via the relay wallet against the basket
   controller.
3. Wait for the controller's payout notes (one per constituent).
4. Consume them — the relay's vault now holds the user's pro-rata share
   of each constituent.
5. POST `/v0/quote` to 1Click with `originAsset=miden-testnet:eth` and
   `recipient=user_evm_addr`.
6. Create the BridgeOutV1 note carrying the bridged ETH.
7. Return correlationId. The 1Click bridge polls, consumes, releases on
   Sepolia.

For M3 we hard-code the constituent -> wei conversion. a future iteration reads live
Pragma medians for accurate pro-rata.

### identity mapping (postgres or sqlite)

```
table positions {
  user_evm_addr      TEXT primary key,
  basket_symbol      TEXT,
  basket_amount      NUMERIC,
  inbound_correlation_id TEXT,
  last_updated       TIMESTAMP
}
table inbound_intents {
  correlation_id     TEXT primary key,
  user_evm_addr      TEXT,
  basket_symbol      TEXT,
  amount_in          NUMERIC,
  status             TEXT,   -- QUOTED | NOTE_CONSUMED | DEPOSIT_SUBMITTED | SETTLED
  created_at         TIMESTAMP
}
```

The user enters their basket symbol *before* requesting the 1Click quote
from our frontend. The frontend posts the intent (basket symbol, user EVM
address, amount) to the relay, which returns a correlationId that the
1Click `/v0/quote` call then carries through. The 1Click `recipient` is
the relay wallet's Miden id; the `refundTo` is the user's EVM address;
the `correlationId` ties them together.

## Frontend integration

The 1Click tab (`OneClickDepositPanel`) currently:

- Quotes 1Click directly with the user's own Miden wallet as `recipient`.

v2 flips it to:

- POST `/v0/intents` to darwin-relay-v2 first with `{ user_evm_addr, basket_symbol, amount_in }`.
- Receive `{ correlation_id, relay_miden_address }`.
- Quote 1Click with `recipient = relay_miden_address` and metadata
  carrying the correlation_id.
- User sends the Sepolia tx.
- POST `/v0/deposit/submit` to 1Click as today.
- Poll `/v0/intents/<correlation_id>` on darwin-relay-v2 for status
  through `SETTLED`.

## Components NOT in v2

- ❌ No more Sepolia escrow contract.
- ❌ No more wDCC/wDAG/wDCO ERC20 (those Sepolia contracts can stay
  deployed for now to avoid breaking links, but the frontend stops
  routing through them).
- ❌ No more "65s relay" UX claim; replaced with "~40 s 1Click + a few
  seconds atomic_deposit".

## What stays from v1

- Pragma price discovery scripts (`darwin-sdk/rust/scripts/`) — orthogonal.
- The Uniswap quote fallback (`rebalance_via_uniswap.sh`) — Flow B
  rebalance leg, separate concern.
- Sepolia `DarwinStrategy` registry (`0x635E19c6…`) — used for basket
  config, not for the deposit path. Stays.

## Migration plan

1. **Done in this commit** — drop the "Ethereum (Sepolia)" tab in the
   frontend (this kills the user-facing v1 entrypoint).
2. **Next iteration** — implement the v2 binary. Reuse v1's tokio
   service skeleton (`src/service.rs`, `src/state.rs`, the postgres
   store). Strip the ETH escrow listener; add the Miden listener +
   1Click client + outbound REST.
3. **Live test** — same dev key, same demo wallet, run end-to-end
   through Bali agglayer (when ready) and through Brian's mock (now).

## Open questions / TODOs

- How the relay attests positions to the user (signed receipt?).
- Whether to do per-user Miden accounts (one relay account per ETH user,
  derived deterministically) or a single shared relay account with a
  storage-map positions ledger. Shared simpler, per-user gives users
  optional self-custody graduation.
- Failure modes: 1Click delivers but atomic_deposit_note fails — refund
  via 1Click reverse? Hold and retry?
- Liveness: the relay is a single-process custodian. Multi-instance with
  leader election will land post-M3.
