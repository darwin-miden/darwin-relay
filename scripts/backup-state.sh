#!/usr/bin/env bash
#
# darwin-relay v2 — point-in-time backup of all stateful surfaces.
#
# Captures:
#   1. The relay wallet's keystore (Falcon-512 private keys)
#      — losing this = wallet inaccessible = custodial assets lost.
#   2. The miden-client store (notes, accounts, blockchain headers)
#      — losing this = re-sync from genesis, slow & risky.
#   3. The relay ledger (intents, redemptions, positions,
#      pending_bridge_outs, worker_heartbeats) — losing this = the
#      relay loses track of what users hold off-chain vs. on-chain.
#
# Produces a single timestamped tarball
# `darwin-relay-state-<unix>.tar.gz` in $BACKUP_DIR (default `./backups`).
# Optionally syncs to a remote when $BACKUP_REMOTE is set
# (any rclone target: s3:bucket/path, b2:bucket, gcs:bucket, etc.).
#
# Recommended cron:
#   0 */6 * * *  /path/to/darwin-relay/scripts/backup-state.sh
#
# Recovery: extract the tarball into the same paths, restart relay
# REST + worker, verify worker heartbeat at /v0/worker-health.
#
# Env (all optional, sensible defaults shown):
#   BACKUP_DIR      ./backups
#   BACKUP_REMOTE   (unset = local only)
#   MIDEN_HOME      ~/.miden
#   RELAY_STORE     /tmp/darwin-relay/relay-v2.sqlite (or ./relay-v2.sqlite)
#   KEEP_LOCAL      14   (rolling number of local tarballs to retain)
#
set -euo pipefail

ts=$(date +%s)
BACKUP_DIR="${BACKUP_DIR:-./backups}"
MIDEN_HOME="${MIDEN_HOME:-$HOME/.miden}"
RELAY_STORE="${RELAY_STORE:-/tmp/darwin-relay/relay-v2.sqlite}"
KEEP_LOCAL="${KEEP_LOCAL:-14}"

# Resolve relay store fallback to the in-repo path if /tmp is empty.
if [[ ! -f "$RELAY_STORE" ]]; then
  if [[ -f "./relay-v2.sqlite" ]]; then
    RELAY_STORE="./relay-v2.sqlite"
  else
    echo "fatal: no relay sqlite found at $RELAY_STORE or ./relay-v2.sqlite" >&2
    exit 1
  fi
fi
if [[ ! -d "$MIDEN_HOME/keystore" ]]; then
  echo "fatal: keystore not found at $MIDEN_HOME/keystore" >&2
  exit 1
fi
if [[ ! -f "$MIDEN_HOME/store.sqlite3" ]]; then
  echo "warn: miden-client store not found at $MIDEN_HOME/store.sqlite3 — backing up keystore + relay only" >&2
fi

mkdir -p "$BACKUP_DIR"

# Stage everything in a temp dir so the tarball has clean paths.
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/keystore"
cp -R "$MIDEN_HOME/keystore"/. "$tmp/keystore/"
if [[ -f "$MIDEN_HOME/store.sqlite3" ]]; then
  # SQLite copy via `.backup` is concurrency-safe (won't read a torn
  # snapshot if the worker is mid-write). Falls back to plain cp if
  # sqlite3 isn't on PATH — the worker writes are small + atomic in
  # WAL mode, so the risk window is tiny.
  if command -v sqlite3 >/dev/null 2>&1; then
    sqlite3 "$MIDEN_HOME/store.sqlite3" ".backup $tmp/miden-store.sqlite3"
  else
    cp "$MIDEN_HOME/store.sqlite3" "$tmp/miden-store.sqlite3"
  fi
fi
if command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 "$RELAY_STORE" ".backup $tmp/relay-v2.sqlite"
else
  cp "$RELAY_STORE" "$tmp/relay-v2.sqlite"
fi

# Tiny manifest so a future operator knows what hosts/paths the
# tarball came from. Helps when restoring on a different machine.
cat > "$tmp/MANIFEST" <<EOF
darwin-relay-state backup
  taken_at:    $(date -u +%FT%TZ) (unix $ts)
  source_host: $(hostname)
  miden_home:  $MIDEN_HOME
  relay_store: $RELAY_STORE
  contents:
    keystore/             # Falcon-512 wallet keys
    miden-store.sqlite3   # miden-client cache
    relay-v2.sqlite       # relay REST ledger
EOF

out="$BACKUP_DIR/darwin-relay-state-$ts.tar.gz"
tar -C "$tmp" -czf "$out" .
echo "✓ wrote $out ($(du -h "$out" | awk '{print $1}'))"

# Optional remote sync via rclone (s3, b2, gcs, etc).
if [[ -n "${BACKUP_REMOTE:-}" ]]; then
  if command -v rclone >/dev/null 2>&1; then
    rclone copy "$out" "$BACKUP_REMOTE" --quiet
    echo "✓ synced to $BACKUP_REMOTE"
  else
    echo "warn: BACKUP_REMOTE set but rclone not on PATH — skipping" >&2
  fi
fi

# Roll old local tarballs.
if [[ -d "$BACKUP_DIR" ]]; then
  to_drop=$(ls -1t "$BACKUP_DIR"/darwin-relay-state-*.tar.gz 2>/dev/null | tail -n +$((KEEP_LOCAL + 1)))
  if [[ -n "$to_drop" ]]; then
    echo "$to_drop" | xargs rm -f
    echo "✓ pruned $(echo "$to_drop" | wc -l | tr -d ' ') old tarball(s), keeping last $KEEP_LOCAL"
  fi
fi
