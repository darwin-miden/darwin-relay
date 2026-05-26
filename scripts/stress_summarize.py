#!/usr/bin/env python3
"""
Summarize a stress run.

Input: a TSV produced by stress_test.sh (header on line 1) and an
optional relay log file containing `RelayDepositSettled` lines that
include the request_tx so request→settle latency can be matched up.

Output: a short table with submit-side stats (p50/p90/p99 submit
seconds, ok/fail counts) and, if the relay log is provided, the
end-to-end submit→settle stats per basket.

Usage:
  ./scripts/stress_summarize.py results/stress-scale-1779100000.tsv \
      --relay-log /var/log/darwin-relay.log
"""
import argparse
import collections
import csv
import re
import statistics
import sys
from pathlib import Path


def pct(values, p):
    if not values:
        return float("nan")
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("tsv", type=Path)
    ap.add_argument("--relay-log", type=Path, default=None)
    args = ap.parse_args()

    rows = []
    with args.tsv.open() as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            rows.append(r)

    if not rows:
        print(f"no rows in {args.tsv}", file=sys.stderr)
        return 1

    ok = [r for r in rows if r["request_tx"] not in ("", "FAIL", "null", None)]
    fail = [r for r in rows if r not in ok]

    print(f"== {args.tsv.name} ==")
    print(f"submitted: ok={len(ok)} fail={len(fail)} total={len(rows)}")
    if "submit_seconds" in rows[0]:
        submit = [int(r["submit_seconds"]) for r in ok if r.get("submit_seconds", "").isdigit()]
        if submit:
            print(f"submit latency: p50={pct(submit, 50)}s "
                  f"p90={pct(submit, 90)}s p99={pct(submit, 99)}s "
                  f"max={max(submit)}s")

    if args.relay_log and args.relay_log.exists():
        # Match request_tx → first observed Settled timestamp.
        by_tx = {r["request_tx"].lower(): r for r in ok}
        settled = {}
        ts_pat = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})")
        with args.relay_log.open(errors="ignore") as f:
            for line in f:
                if "RelayDepositSettled" not in line and "settled" not in line.lower():
                    continue
                m = re.search(r"(0x[0-9a-fA-F]{64})", line)
                ts = ts_pat.search(line)
                if not m or not ts:
                    continue
                tx = m.group(1).lower()
                if tx in by_tx and tx not in settled:
                    settled[tx] = ts.group(1)

        latencies_by_basket = collections.defaultdict(list)
        for tx, t_settle in settled.items():
            row = by_tx[tx]
            # The TSV only records request submit_seconds, not absolute
            # submit time, so we report settle clock per-basket here.
            latencies_by_basket[row["basket"][:10] + "…"].append(t_settle)

        print(f"settled: {len(settled)}/{len(ok)} matched in log")
        for b, ts in sorted(latencies_by_basket.items()):
            print(f"  basket {b}: {len(ts)} settled, last={ts[-1]}")
    else:
        print("(no --relay-log given; skipping settle-side stats)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
