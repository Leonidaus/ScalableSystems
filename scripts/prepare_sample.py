#!/usr/bin/env python3
"""Build the correctness sample: all lines of a stratified set of symbols
from one DEBS day, pre-sorted by event time (Trading time + Trading date,
fallback Date), so no event is ever late for its window.

Picks head symbols, mid-tail, sparse (1-5 trades a day) and bid/ask-only
symbols, to cover the dataset's long-tail distribution.

Usage: prepare_sample.py <day.csv> <out-sample.csv>
"""
import sys
from pathlib import Path

LAST = 21


def count_activity(path):
    # symbol -> [trades, lines]
    counts = {}
    with open(path) as f:
        for line in f:
            s = line[: line.find(",")]
            if not s or line.startswith(("ID,", "#")):
                continue
            rec = counts.setdefault(s, [0, 0])
            rec[1] += 1
            try:
                if float(line.split(",", LAST + 1)[LAST]) > 0:
                    rec[0] += 1
            except (ValueError, IndexError):
                pass
    return counts


def pick_symbols(counts, max_lines=800_000):
    by_trades = sorted(counts, key=lambda s: (-counts[s][0], s))
    trades, lines = {s: c[0] for s, c in counts.items()}, {s: c[1] for s, c in counts.items()}
    out = []

    def take(syms):
        for s in syms:
            if s not in out and (not out or sum(lines[x] for x in out) < max_lines):
                out.append(s)

    mid = [s for s in by_trades if 5 < trades[s] <= 1000]
    take(sorted(s for s in counts if trades[s] == 0)[:2])   # bid/ask-only
    take(by_trades[:8])                                     # head
    take(sorted(s for s in counts if 1 <= trades[s] <= 5)[:12])  # sparse
    if mid:
        take([mid[int(r * (len(mid) - 1) / 11)] for r in range(12)])  # mid tail
    take(by_trades[8:14])
    return out


def event_key(cols):
    # Trading time + Trading date, else Date; unparseable lines sort last.
    date, t, tdate = cols[2], cols[23], cols[26].rstrip("\r")
    eff = tdate or date
    day = eff[6:10] + eff[3:5] + eff[0:2] if len(eff) == 10 and eff[2] == "-" and eff[5] == "-" else "99999999"
    t = t[:8] if len(t) >= 8 and t[2] == ":" and t[5] == ":" else "99:99:99"
    return day + t[:8] + ("\x01" + cols[23][8:] if t == cols[23][:8] and cols[23][8:9] == "." else "")


def main():
    day, out = Path(sys.argv[1]), Path(sys.argv[2])

    counts = count_activity(day)
    symbols = pick_symbols(counts)
    print(f"selected {len(symbols)} symbols, ~{sum(counts[s][1] for s in symbols)} lines")
    for s in symbols:
        print(f"  {s}: {counts[s][0]} trades, {counts[s][1]} lines")

    symbol_set = set(symbols)
    lines, header = [], None
    with open(day) as f:
        for line in f:
            if header is None and line.startswith("ID,"):
                header = line
            elif not line.startswith(("ID,", "#")):
                if line[: line.find(",")] in symbol_set:
                    lines.append(line)

    def key(line):
        cols = line.split(",")
        return event_key(cols) if len(cols) > 26 else "\x7f"

    lines.sort(key=key)

    with open(out, "w") as g:
        if header:
            g.write(header)
        g.writelines(lines)
    print(f"wrote {out}: {len(lines)} lines")


if __name__ == "__main__":
    main()
