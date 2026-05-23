#!/usr/bin/env bash
#
# Full v2 pipeline demo. Drives a deposit + redeem through both the axum
# service AND the on-chain worker, end to end:
#
#   1. POST /v0/intents (axum)
#   2. 1Click /v0/quote
#   3. cast send Sepolia
#   4. POST /v0/deposit/submit (1Click) + POST /v0/intents/:id/deposit (axum)
#   5. Wait for stage = POSITION_CREDITED
#   6. Wait for atomic_deposit_tx (worker)
#   7. POST /v0/redeem
#   8. Wait for miden_redeem_tx (worker)
#   9. Wait for miden_bridge_out_tx (worker)
#  10. Wait for sepolia_release_tx + stage = FULLY_SETTLED
#
# Prereqs:
#   - darwin_relay_v2 running         (:8090)
#   - darwin_relay_v2_worker running  (separate shell)
#   - 1Click mock running             (:8080)
#   - cast on $PATH
#   - DARWIN_DEV_KEY exported (funded Sepolia EOA)
#
set -euo pipefail

RELAY_URL="${DARWIN_RELAY_V2_URL:-http://localhost:8090}"
ONECLICK_URL="${ONECLICK_URL:-http://localhost:8080}"
RPC_URL="${SEPOLIA_RPC_URL:-https://ethereum-sepolia.publicnode.com}"
BASKET="${DARWIN_BASKET:-DCC}"
AMOUNT_WEI="${DARWIN_AMOUNT_WEI:-300000000000}"

if [[ -z "${DARWIN_DEV_KEY:-}" ]]; then
  echo "DARWIN_DEV_KEY is required (funded Sepolia EOA)" >&2
  exit 1
fi
USER_ADDR=$(cast wallet address --private-key "$DARWIN_DEV_KEY")

echo "relay   = $RELAY_URL"
echo "1Click  = $ONECLICK_URL"
echo "user    = $USER_ADDR"
echo "basket  = $BASKET"
echo "amount  = $AMOUNT_WEI wei"
echo

wait_field() {
  # $1 = description, $2 = sql, $3 = timeout-iters (15s each)
  local desc="$1" sql="$2" tries="${3:-40}"
  for i in $(seq 1 "$tries"); do
    v=$(sqlite3 "${DARWIN_RELAY_V2_STORE:-./relay-v2.sqlite}" "$sql")
    if [[ -n "$v" && "$v" != "" ]]; then
      printf "  ✓ %s = %s\n" "$desc" "$v"
      return 0
    fi
    printf "  [%2ds] %s pending…\n" $((i*15)) "$desc"
    sleep 15
  done
  echo "  ✗ $desc never set after $((tries*15))s" >&2
  return 1
}

echo "==> 1) POST /v0/intents"
INTENT=$(curl -sf -X POST "$RELAY_URL/v0/intents" -H 'content-type: application/json' \
  -d "{\"user_evm_addr\":\"$USER_ADDR\",\"basket_symbol\":\"$BASKET\",\"amount_in_wei\":\"$AMOUNT_WEI\"}")
CID=$(echo "$INTENT" | python3 -c "import sys,json; print(json.load(sys.stdin)['correlation_id'])")
RELAY_ADDR=$(echo "$INTENT" | python3 -c "import sys,json; print(json.load(sys.stdin)['relay_miden_address'])")
echo "   intent = $CID  relay = $RELAY_ADDR"

echo "==> 2) POST 1Click /v0/quote"
QUOTE=$(curl -sf -X POST "$ONECLICK_URL/v0/quote" -H 'content-type: application/json' \
  -d "{\"dry\":false,\"depositMode\":\"SIMPLE\",\"swapType\":\"EXACT_INPUT\",\"slippageTolerance\":100.0,\"originAsset\":\"eth-sepolia:eth\",\"depositType\":\"ORIGIN_CHAIN\",\"destinationAsset\":\"miden-testnet:eth\",\"amount\":\"$AMOUNT_WEI\",\"refundTo\":\"$USER_ADDR\",\"refundType\":\"ORIGIN_CHAIN\",\"recipient\":\"$RELAY_ADDR\",\"recipientType\":\"DESTINATION_CHAIN\",\"deadline\":\"2027-01-01T00:00:00Z\"}")
DEP_ADDR=$(echo "$QUOTE" | python3 -c "import sys,json; print(json.load(sys.stdin)['quote']['depositAddress'])")
echo "   deposit = $DEP_ADDR"

echo "==> 3) cast send sepolia"
TX=$(cast send "$DEP_ADDR" --value "$AMOUNT_WEI" --private-key "$DARWIN_DEV_KEY" --rpc-url "$RPC_URL" 2>&1 | grep -E "^transactionHash" | head -1 | awk '{print $2}')
echo "   tx = $TX"

echo "==> 4) notify both sides"
curl -sf -X POST "$ONECLICK_URL/v0/deposit/submit" -H 'content-type: application/json' \
  -d "{\"txHash\":\"$TX\",\"depositAddress\":\"$DEP_ADDR\"}" >/dev/null
curl -sf -X POST "$RELAY_URL/v0/intents/$CID/deposit" -H 'content-type: application/json' \
  -d "{\"deposit_address\":\"$DEP_ADDR\",\"sepolia_tx\":\"$TX\"}" >/dev/null

echo "==> 5) wait for POSITION_CREDITED"
wait_field "stage" "SELECT stage FROM intents WHERE correlation_id='$CID' AND stage='POSITION_CREDITED'" 20

echo "==> 6) wait for worker to set atomic_deposit_tx"
wait_field "atomic_deposit_tx" "SELECT atomic_deposit_tx FROM intents WHERE correlation_id='$CID' AND atomic_deposit_tx IS NOT NULL" 12

echo "==> 7) POST /v0/redeem (half the amount)"
HALF=$(( (AMOUNT_WEI * 9_970 / 10_000) / 2 ))
REDEEM=$(curl -sf -X POST "$RELAY_URL/v0/redeem" -H 'content-type: application/json' \
  -d "{\"user_evm_addr\":\"$USER_ADDR\",\"basket_symbol\":\"$BASKET\",\"basket_amount\":\"$HALF\"}")
RID=$(echo "$REDEEM" | python3 -c "import sys,json; print(json.load(sys.stdin)['redemption_id'])")
echo "   redemption = $RID"

echo "==> 8) wait for miden_redeem_tx"
wait_field "miden_redeem_tx" "SELECT miden_redeem_tx FROM redemptions WHERE redemption_id='$RID' AND miden_redeem_tx IS NOT NULL" 12

echo "==> 9) wait for miden_bridge_out_tx"
wait_field "miden_bridge_out_tx" "SELECT miden_bridge_out_tx FROM redemptions WHERE redemption_id='$RID' AND miden_bridge_out_tx IS NOT NULL" 12

echo "==> 10) wait for sepolia_release_tx + FULLY_SETTLED"
wait_field "sepolia_release_tx" "SELECT sepolia_release_tx FROM redemptions WHERE redemption_id='$RID' AND sepolia_release_tx IS NOT NULL" 40

echo
echo "✓ Full pipeline complete."
echo "   intent      = $CID"
echo "   redemption  = $RID"
echo
echo "   sqlite> SELECT * FROM intents     WHERE correlation_id='$CID';"
echo "   sqlite> SELECT * FROM redemptions WHERE redemption_id='$RID';"
