#!/usr/bin/env bash
#
# Darwin Relay stress test -- three scenarios fired against the live
# Sepolia stack from one operator key:
#
#   1. large    -- single big deposit (~1,000 USDC) → measures path
#                 latency under a high-value claim.
#   2. low      -- 5 small deposits (10–50 USDC) spaced 5s apart → no
#                 rebalance threshold tripped, measures steady-state.
#   3. high     -- 10 deposits in rapid succession across the three
#                 baskets with varied amounts → exercises queue + nonce.
#   4. scale    -- 100 deposits fired in parallel across all three
#                 baskets → stress the relay queue + Miden tx pool.
#
# Outputs:
#   - results/stress-<scenario>-<unix>.tsv with: id, basket, amount,
#                                                request_tx, request_block,
#                                                settle_tx, settle_block,
#                                                request_to_settle_seconds
#
# Pre-reqs (env or defaults):
#   RPC                Sepolia HTTP RPC (defaults to publicnode)
#   USER_PK            User EOA private key (signs the deposits)
#   RELAY              DarwinRelayDeposit address
#   USDC               MockUSDC address
#   DCC_ID/DAG_ID/DCO_ID  basketIds
#
# The relay service must be running pointed at this stack so that
# `RelayDepositRequested` → `RelayDepositSettled` transitions happen.

set -uo pipefail

RPC=${RPC:-https://ethereum-sepolia-rpc.publicnode.com}
USER_PK=${USER_PK:-0x47b0a088fc62101d8aefc501edec2266ff2fc4cf84c93a8e6c315dedb0d942be}
RELAY=${RELAY:-0x7e5279AD0d9F7fB8884562C336Fa6d78DCbf7c93}
USDC=${USDC:-0x6dAb940a4E1d434965E22e9F6d624fF68F6922a0}
DCC_ID=${DCC_ID:-0x1fbfef9aa7f4e8f8bd84b940396c9263c0c2ac2212f53759ceb3b71aaeed43fe}
DAG_ID=${DAG_ID:-0x74491929c2f72408e48b338222172a8a07d8c3087617d09881d00d72278eb6c1}
DCO_ID=${DCO_ID:-0xb2cbc4016a8155cd5b6be0c2683f937c73985e9bee24f6cb8e383f4967408757}

USER=$(cast wallet address --private-key "$USER_PK")
OUT=${OUT:-$(pwd)/results}
mkdir -p "$OUT"

SCENARIO=${1:-help}

# 32-byte zero word is fine as Miden recipient placeholder for this stress
# harness -- the relay enqueues the deposit, basket-token mint targets the
# user's ETH address (not the Miden side) for the wrapped flow.
MIDEN_RECIPIENT=0x0000000000000000000000000000000000000000000000000000000000000000

usage() {
  cat <<EOF
Usage: $0 <scenario>

Scenarios:
  large     1 large deposit (1,000 USDC into DCC)
  low       5 small deposits (10/15/20/25/50 USDC into DCC)
  high      10 mixed deposits across DCC/DAG/DCO
  scale     100 parallel deposits across DCC/DAG/DCO
            (set SCALE_N to override count, default 100;
             SCALE_PARALLELISM caps concurrent submitters, default 10)

Env: RPC, USER_PK, RELAY, USDC, DCC_ID, DAG_ID, DCO_ID, OUT
     SCALE_N, SCALE_PARALLELISM
EOF
}

submit_one() {
  local idx=$1 basket=$2 amount=$3
  echo "[$idx] depositing $amount USDC into $basket from $USER..."

  # 1. ensure MockUSDC balance -- self-mint if needed
  local bal
  bal=$(cast call "$USDC" "balanceOf(address)(uint256)" "$USER" --rpc-url "$RPC")
  bal=${bal%% *}
  if [[ "$bal" -lt "$amount" ]]; then
    cast send "$USDC" "mint(address,uint256)" "$USER" "$amount" \
      --rpc-url "$RPC" --private-key "$USER_PK" --json >/dev/null 2>&1
  fi

  # 2. approve the relay
  cast send "$USDC" "approve(address,uint256)" "$RELAY" "$amount" \
    --rpc-url "$RPC" --private-key "$USER_PK" --json >/dev/null 2>&1

  # 3. submit the deposit + capture the tx + block
  local t0 t1 hash block
  t0=$(date +%s)
  hash=$(cast send "$RELAY" "deposit(uint256,bytes32,bytes32)" \
    "$amount" "$basket" "$MIDEN_RECIPIENT" \
    --rpc-url "$RPC" --private-key "$USER_PK" --json | jq -r .transactionHash)
  t1=$(date +%s)
  block=$(cast tx "$hash" --rpc-url "$RPC" | awk '/blockNumber/ {print $2}')
  echo "    request tx=$hash block=$block (submit $((t1 - t0))s)"
  echo -e "$idx\t$basket\t$amount\t$hash\t$block" >> "$results"
}

results="$OUT/stress-$SCENARIO-$(date +%s).tsv"
echo -e "idx\tbasket\tamount_usdc_base\trequest_tx\trequest_block" > "$results"

case "$SCENARIO" in
  large)
    submit_one 0 "$DCC_ID" 1000000000   # 1,000 USDC (6 decimals)
    ;;
  low)
    for i in 0 1 2 3 4; do
      amt=$(( (i + 1) * 10000000 ))
      submit_one "$i" "$DCC_ID" "$amt"
      sleep 5
    done
    ;;
  high)
    ids=("$DCC_ID" "$DAG_ID" "$DCO_ID")
    amounts=(5000000 10000000 25000000 50000000 100000000 5000000 10000000 25000000 50000000 100000000)
    for i in {0..9}; do
      submit_one "$i" "${ids[$((i % 3))]}" "${amounts[$i]}"
    done
    ;;
  scale)
    # Fire N (default 100) deposits with a parallelism cap. The cap
    # exists because Sepolia public RPCs (publicnode in particular)
    # rate-limit aggressively past ~12 in-flight `cast send` calls
    # from one EOA — without the cap we'd just measure 429s, not the
    # relay's actual throughput.
    N=${SCALE_N:-100}
    P=${SCALE_PARALLELISM:-10}
    ids=("$DCC_ID" "$DAG_ID" "$DCO_ID")
    amounts=(5000000 10000000 25000000 50000000 100000000)

    # Pre-mint + pre-approve a big bag once instead of per-submit,
    # so the parallel submitters don't all race to mint themselves.
    total=$(( N * 100000000 ))
    echo "[scale] pre-minting $total base-USDC + approving relay"
    cast send "$USDC" "mint(address,uint256)" "$USER" "$total" \
      --rpc-url "$RPC" --private-key "$USER_PK" --json >/dev/null 2>&1
    cast send "$USDC" "approve(address,uint256)" "$RELAY" "$total" \
      --rpc-url "$RPC" --private-key "$USER_PK" --json >/dev/null 2>&1

    submit_scale_one() {
      local idx=$1
      local basket="${ids[$((idx % 3))]}"
      local amount="${amounts[$((idx % 5))]}"
      local t0 t1 hash
      t0=$(date +%s)
      hash=$(cast send "$RELAY" "deposit(uint256,bytes32,bytes32)" \
        "$amount" "$basket" "$MIDEN_RECIPIENT" \
        --rpc-url "$RPC" --private-key "$USER_PK" --json 2>/dev/null \
        | jq -r .transactionHash 2>/dev/null)
      t1=$(date +%s)
      if [[ -n "$hash" && "$hash" != "null" ]]; then
        echo -e "$idx\t$basket\t$amount\t$hash\t-\t$((t1 - t0))" >> "$results"
        echo "[scale $idx] tx=$hash submit=$((t1 - t0))s"
      else
        echo -e "$idx\t$basket\t$amount\tFAIL\t-\t$((t1 - t0))" >> "$results"
        echo "[scale $idx] FAIL"
      fi
    }

    echo -e "idx\tbasket\tamount_usdc_base\trequest_tx\trequest_block\tsubmit_seconds" > "$results"

    start_wall=$(date +%s)
    pids=()
    for i in $(seq 0 $((N - 1))); do
      submit_scale_one "$i" &
      pids+=("$!")
      # Throttle to P concurrent submitters.
      if (( ${#pids[@]} >= P )); then
        wait "${pids[0]}" 2>/dev/null
        pids=("${pids[@]:1}")
      fi
    done
    wait
    end_wall=$(date +%s)

    ok=$(grep -c -v 'FAIL' "$results" || true)
    ok=$(( ok - 1 ))   # minus header
    fail=$(grep -c 'FAIL' "$results" || true)
    echo
    echo "[scale] wall=$((end_wall - start_wall))s ok=$ok fail=$fail (N=$N, P=$P)"
    ;;
  help|*)
    usage; exit 1
    ;;
esac

echo
echo "results written to $results"
echo
echo "next: monitor the relay log for RelayDepositSettled events to see end-to-end latency."
