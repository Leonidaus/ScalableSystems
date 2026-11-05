#!/usr/bin/env python
"""Diff the reference output against the job's actual output. Exit 1 on mismatch."""
import csv
import sys


def load(path, skip_header):
    rows = {}
    with open(path) as f:
        if skip_header:
            next(f)
        for row in csv.reader(f):
            if row:
                rows[(row[0], int(row[1]))] = row
    return rows


def main():
    ref = load(sys.argv[1], skip_header=True)
    act = load(sys.argv[2], skip_header=False)

    ok = True
    for key in sorted(set(ref) | set(act)):
        r, a = ref.get(key), act.get(key)
        if r is None or a is None:
            print(f"{key[0]} {key[1]}: only in {'actual' if a is None else 'reference'}")
            ok = False
            continue
        for i, col in ((3, "close"), (4, "ema38"), (5, "ema100")):
            if abs(float(r[i]) - float(a[i])) > 1e-9:
                print(f"{key[0]} {key[1]}: {col} {r[i]} != {a[i]}")
                ok = False
        adv_ref, adv_act = r[6] or "", a[6] or ""
        if adv_ref != adv_act:
            print(f"{key[0]} {key[1]}: advisory {adv_ref!r} != {adv_act!r}")
            ok = False

    print("PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
