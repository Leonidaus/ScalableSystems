#!/usr/bin/env python
"""Independent implementation of the two queries, for checking the job."""
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

PARIS = ZoneInfo("Europe/Paris")


def parse_time(t):
    # first fraction digits weigh 100/10/1 ms, the rest is ignored
    if len(t) < 8 or t[2] != ":" or t[5] != ":":
        return -1
    if not (t[0:2].isdigit() and t[3:5].isdigit() and t[6:8].isdigit()):
        return -1
    h, m, s = int(t[0:2]), int(t[3:5]), int(t[6:8])
    ms, scale = 0, 100
    if len(t) > 9 and t[8] == ".":
        for c in t[9:]:
            if scale == 0:
                break
            if not c.isdigit():
                return -1
            ms += int(c) * scale
            scale //= 10
    return ((h * 60 + m) * 60 + s) * 1000 + ms


def day_millis(d):
    if len(d) != 10 or d[2] != "-" or d[5] != "-":
        return None
    try:
        day = datetime(int(d[6:10]), int(d[3:5]), int(d[0:2]), tzinfo=PARIS)
        return int(day.timestamp() * 1000)
    except ValueError:
        return None


def main():
    src, dst = sys.argv[1], sys.argv[2]

    # (symbol, window) -> (ts, close); ties keep the first, like the job
    closes = {}
    for line in open(src):
        if line.startswith(("ID,", "#")):
            continue
        cols = line.split(",")
        if len(cols) < 27 or cols[23] == "":
            continue
        try:
            price = float(cols[21])
        except ValueError:
            continue
        if not price > 0:
            continue
        ts = parse_time(cols[23])
        day = day_millis(cols[26] or cols[2])
        if ts < 0 or day is None or cols[0] == "":
            continue
        key = (cols[0], (day + ts) // 300000)
        prev = closes.get(key)
        if prev is None or ts > prev[0]:
            closes[key] = (ts, price)

    windows = {}
    for (s, w), pair in closes.items():
        windows.setdefault(s, []).append((w,) + pair)

    total = sum(len(ws) for ws in windows.values())
    with open(dst, "w") as f:
        f.write("symbol,window_start,window_end,close,ema38,ema100,advisory\n")
        for s in sorted(windows):
            e38 = e100 = 0.0
            for w, ts, price in sorted(windows[s]):
                prev38, prev100 = e38, e100
                e38 = price * 2 / 39 + prev38 * 37 / 39
                e100 = price * 2 / 101 + prev100 * 99 / 101
                if e38 > e100 and prev38 <= prev100:
                    adv = "BUY"
                elif e38 < e100 and prev38 >= prev100:
                    adv = "SELL"
                else:
                    adv = ""
                f.write(f"{s},{w * 300000},{(w + 1) * 300000},{price},{e38},{e100},{adv}\n")
    print(f"{total} windows -> {dst}")


if __name__ == "__main__":
    main()
