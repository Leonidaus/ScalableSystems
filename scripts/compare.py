#!/usr/bin/env python3
"""Compare the reference implementation's output with the job's actual output
(LocalRun). Print coverage stats for the report. Exit 1 on any mismatch.
Usage: compare.py <reference.csv> <actual.csv> [sample.csv]
"""
import sys

import pandas as pd


def main():
    ref = pd.read_csv(sys.argv[1])
    act = pd.read_csv(
        sys.argv[2],
        names=["symbol", "window_start", "window_end", "close", "ema38", "ema100", "advisory"],
    )
    m = ref.merge(act, on=["symbol", "window_start"], how="outer",
                  suffixes=("_ref", "_act"), indicator=True)
    both = m[m._merge == "both"].fillna({"advisory_ref": "", "advisory_act": ""})

    ok = True
    if (m._merge != "both").any():
        print("rows only on one side:")
        print(m[m._merge != "both"][["symbol", "window_start", "_merge"]].head(10).to_string(index=False))
        ok = False
    for col in ("close", "ema38", "ema100"):
        a, b = both[col + "_ref"], both[col + "_act"]
        bad = (a - b).abs() > 1e-9
        if bad.any():
            print(f"{col} mismatches: {bad.sum()}")
            print(both[bad].head(10).to_string(index=False))
            ok = False
    bad = both[both.advisory_ref != both.advisory_act]
    if len(bad):
        print(f"advisory mismatches: {len(bad)}")
        print(bad.head(10).to_string(index=False))
        ok = False

    per = both.groupby("symbol").window_start.nunique()
    span = both.groupby("symbol").window_start.agg(["min", "max"])
    print(f"windows: {len(both)}/{len(ref)} matched over {both.symbol.nunique()} symbols")
    print(f"windows/symbol: min={per.min()}, median={per.median():.0f}, max={per.max()}")
    print("advisories:", ref.advisory.value_counts().to_dict())
    print("symbols with empty windows in their span (carry-forward):",
          ((span["max"] - span["min"]) // 300000 + 1 > per).sum())

    if len(sys.argv) > 3:
        total = trades = 0
        for line in open(sys.argv[3]):
            if line.startswith(("ID,", "#")) or not line.strip():
                continue
            total += 1
            cols = line.split(",")
            try:
                if len(cols) > 21 and float(cols[21]) > 0:
                    trades += 1
            except ValueError:
                pass
        print(f"sample: {total} lines, {trades} trades, {total - trades} non-trade lines")

    print("PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
