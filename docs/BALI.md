# Bali agglayer integration

The canonical Sepolia ↔ Miden bridge for Darwin. Operated by
gateway-fm via the [miden-agglayer](https://github.com/gateway-fm/miden-agglayer)
stack. Separate from BrianSeong99's 1Click mock (which lives at
`http://localhost:8080` in dev).

## Network params (post-relaunch 2026-05-26)

| field | value |
|---|---|
| Network ID | `76` |
| L2 chain ID | `1022211914` |
| Bech32 HRP | `mcst1` |
| Bridge account (Miden) | `mcst1arychvrurzxdy5qwz0mg5p5umsvsepyx` / `0xc98bb07c188cd2500e13f68a069cdc` |
| ETH faucet (Miden) | `mcst1arnrhfau9svl7cpu2tr8lfzzd5j87wwe` / `0xe63ba7bc2c19ff603c52c67fa4426d` |
| Sepolia bridge contract | `0x1348947e282138d8f377b467F7D9c2EB0F335d1f` |
| Bridge service API | `https://miden-testnet-bridge.dev.eu-north-3.gateway.fm` |
| Pinned `miden-client` | `0.14.4` (Poseidon2) |

The pre-relaunch network 73 deposits are **permanently frozen**. Anything
that still references `mtst1` HRP, `0xa75ca0c…` bridge, or
`network=73` is from the old outpost and not recoverable.

## L1 → L2 path (Sepolia → Miden)

1. `bridgeAsset(76, dest, amount, 0x0, true, 0x)` on the Sepolia
   bridge contract. `dest` is the 20-byte ETH-padded form of the
   recipient's 15-byte Miden account ID — the frontend's `midenToEthDest()`
   helper handles the padding.
2. Bridge service indexes the deposit (`/api/bridges/:dest`) — visible
   immediately with `ready_for_claim=false`.
3. ~25-30 min later: aggsender pushes the GER, `ready_for_claim`
   flips to `true`, bridge solver mints a P2ID note on Miden carrying
   the ETH (from faucet `0xe63ba7bc…`) to the destination. The note's
   tx hash appears in the deposit row's `claim_tx_hash`.
4. The recipient's miden-client sync picks the note up. The relay v2
   worker auto-drains it into the relay vault on its next tick.

### Decimals

The Bali ETH faucet uses **8 decimals**, not 18. A 0.001 ETH
Sepolia deposit (`10^15` wei) lands as `100000` base units on
Miden (`0.001 * 10^8`).

### Frontend test panel

`darwin-frontend/src/components/BaliDepositPanel.tsx` drives the L1→L2
flow from the browser. Connect an ETH wallet on `/portfolio`, set the
amount and Miden destination, click the bridge button. The panel
polls `/api/bridges/:dest` every 30 s and shows the lifecycle:
`awaiting-wallet → tx-sent → indexing → ready-to-claim → claimed`.

## L2 → L1 path (Miden → Sepolia)

Not wired into the v2 worker yet. Brian's 1Click mock outbound uses
its own `bridge_out_v1` MASM script; the canonical Bali agglayer
outbound uses a different note format (`B2AGG` in the gateway-fm
codebase) requiring the [`bridge-out-tool`](https://github.com/gateway-fm/miden-agglayer/tree/main/scripts)
binary or a custom note builder.

Expected timing per revitteth on `0xMiden/miden-client#2173`:
~30-90 min on the happy path post-relaunch.

## Verification artefacts

First full L1→L2 round-trip on the relaunched outpost (2026-05-26):

- Sepolia tx `0x0e246200f3b0fe345f34cf31d6ecb4154ff39d4a80427412596ec66448fddec3`
  (block 10925457)
- bridge service deposit_cnt `1132814`, dest_net `76`
- Claim tx on Miden `0x9697f4a109e8e80b0d271640931ecef53e2b5a4a13a0cf52878c8408b3348de7`
- ~30 min wall-clock from `cast send` to relay vault credited

## Gotchas

- Sepolia bridge contract is the same on both networks (73 + 76). Only
  the `destinationNetwork` arg distinguishes them. Submitting with
  `73` after the relaunch sends funds to a network where the L2 stack
  is gone — funds frozen, not recoverable.
- The bridge service indexes by **destination address** (the ETH-padded
  form), not by sender. Querying `/api/bridges/:sender_eoa` always
  returns empty for our deposits.
- The trailing zero byte on `midenToEthDest()` is significant — without
  it the bridge can decode the Miden ID but the alignment is wrong and
  the claim note never lands on the right account.
