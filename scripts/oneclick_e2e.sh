#!/usr/bin/env bash
#
# End-to-end driver for darwin-relay v2 against a local 1Click mock.
#
# Walks one deposit through the full state machine:
#   intent claim  -> 1Click quote -> sepolia tx -> dual notify
#     -> poll relay until POSITION_CREDITED
#     -> partial redeem -> verify debited balance
#
# Prerequisites:
#   - darwin_relay_v2 running   (default port: 8090)
#   - 1Click mock running       (default port: 8080)
#   - foundry `cast` on $PATH
#   - DARWIN_DEV_KEY set to a funded Sepolia EOA private key
#
# Usage:
#   DARWIN_DEV_KEY=0x... bash scripts/oneclick_e2e.sh
#
set -euo pipefail

RELAY_URL="${DARWIN_RELAY_V2_URL:-http://localhost:8090}"
ONECLICK_URL="${ONECLICK_URL:-http://localhost:8080}"
RPC_URL="${SEPOLIA_RPC_URL:-https://ethereum-sepolia.publicnode.com}"
USER_ADDR="${DARWIN_USER_ADDR:-}"
BASKET="${DARWIN_BASKET:-DCC}"
AMOUNT_WEI="${DARWIN_AMOUNT_WEI:-300000000000}" # 0.0000003 ETH

if [[ -z "${DARWIN_DEV_KEY:-}" ]]; then
  echo "DARWIN_DEV_KEY is required (funded Sepolia EOA)" >&2
  exit 1
fi

if [[ -z "$USER_ADDR" ]]; then
  USER_ADDR=$(cast wallet address --private-key "$DARWIN_DEV_KEY")
fi

echo "relay   = $RELAY_URL"
echo "1Click  = $ONECLICK_URL"
echo "user    = $USER_ADDR"
echo "basket  = $BASKET"
echo "amount  = $AMOUNT_WEI wei"
echo

echo "==> 1) POST /v0/intents"
INTENT=$(curl -sf -X POST "$RELAY_URL/v0/intents" \
  -H 'content-type: application/json' \
  -d "{\"user_evm_addr\":\"$USER_ADDR\",\"basket_symbol\":\"$BASKET\",\"amount_in_wei\":\"$AMOUNT_WEI\"}")
CID=$(echo "$INTENT" | python3 -c "import sys,json; print(json.load(sys.stdin)['correlation_id'])")
RELAY_ADDR=$(echo "$INTENT" | python3 -c "import sys,json; print(json.load(sys.stdin)['relay_miden_address'])")
echo "   intent  = $CID"
echo "   relay   = $RELAY_ADDR"

echo "==> 2) POST 1Click /v0/quote (recipient = relay)"
QUOTE=$(curl -sf -X POST "$ONECLICK_URL/v0/quote" \
  -H 'content-type: application/json' \
  -d "{\"dry\":false,\"depositMode\":\"SIMPLE\",\"swapType\":\"EXACT_INPUT\",\"slippageTolerance\":100.0,\"originAsset\":\"eth-sepolia:eth\",\"depositType\":\"ORIGIN_CHAIN\",\"destinationAsset\":\"miden-testnet:eth\",\"amount\":\"$AMOUNT_WEI\",\"refundTo\":\"$USER_ADDR\",\"refundType\":\"ORIGIN_CHAIN\",\"recipient\":\"$RELAY_ADDR\",\"recipientType\":\"DESTINATION_CHAIN\",\"deadline\":\"2027-01-01T00:00:00Z\"}")
DEP_ADDR=$(echo "$QUOTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['quote']['depositAddress'])")
echo "   deposit = $DEP_ADDR"

echo "==> 3) cast send sepolia tx"
TX_HASH=$(cast send "$DEP_ADDR" --value "$AMOUNT_WEI" --private-key "$DARWIN_DEV_KEY" --rpc-url "$RPC_URL" 2>&1 | grep -E "^transactionHash" | head -1 | awk '{print $2}')
echo "   tx      = $TX_HASH"

echo "==> 4) notify 1Click + relay"
curl -sf -X POST "$ONECLICK_URL/v0/deposit/submit" \
  -H 'content-type: application/json' \
  -d "{\"txHash\":\"$TX_HASH\",\"depositAddress\":\"$DEP_ADDR\"}" >/dev/null
curl -sf -X POST "$RELAY_URL/v0/intents/$CID/deposit" \
  -H 'content-type: application/json' \
  -d "{\"deposit_address\":\"$DEP_ADDR\",\"sepolia_tx\":\"$TX_HASH\"}" >/dev/null
echo "   notified."

echo "==> 5) poll relay until POSITION_CREDITED (or timeout 5min)"
for i in $(seq 1 20); do
  STAGE=$(curl -sf "$RELAY_URL/v0/intents/$CID" | python3 -c "import sys,json; print(json.load(sys.stdin)['stage'])")
  printf "   [%2ds] %s\n" "$((i * 15))" "$STAGE"
  case "$STAGE" in
    POSITION_CREDITED)
      break
      ;;
    ERROR|REFUNDED|FAILED)
      echo "   ✗ terminal failure stage: $STAGE" >&2
      exit 1
      ;;
  esac
  sleep 15
done

if [[ "$STAGE" != "POSITION_CREDITED" ]]; then
  echo "   ✗ did not credit within 5min (last stage: $STAGE)" >&2
  exit 1
fi

echo "==> 6) GET /v0/positions"
curl -sf "$RELAY_URL/v0/positions/$USER_ADDR" | python3 -m json.tool

echo "==> 7) redeem half"
HALF=$((AMOUNT_WEI / 2))
REDEEM=$(curl -sf -X POST "$RELAY_URL/v0/redeem" \
  -H 'content-type: application/json' \
  -d "{\"user_evm_addr\":\"$USER_ADDR\",\"basket_symbol\":\"$BASKET\",\"basket_amount\":\"$HALF\"}")
echo "$REDEEM" | python3 -m json.tool

echo "==> 8) verify position is now AMOUNT - HALF"
curl -sf "$RELAY_URL/v0/positions/$USER_ADDR" | python3 -m json.tool

echo
echo "✓ E2E ok."
