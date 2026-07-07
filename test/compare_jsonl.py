#!/usr/bin/env python3
"""Compare two JSONL files as multisets of records.

Tolerant of the null/absent-key artifacts VisiData introduces when saving
sparse data: within each record, keys whose value is None or '' are dropped
before comparison (VisiData materializes a union of columns and fills absent
cells with null/empty on save, whereas `cpd` omits absent keys). Records are
then compared as an order-insensitive multiset.

Usage: compare_jsonl.py EXPECTED.jsonl ACTUAL.jsonl
Exit 0 if equal, 1 with a diff report on stderr otherwise.
"""
import json
import sys
from collections import Counter


def load(path):
    recs = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if isinstance(obj, dict):
                obj = {k: v for k, v in obj.items() if v is not None and v != ""}
            recs.append(json.dumps(obj, sort_keys=True, ensure_ascii=False))
    return Counter(recs)


def main():
    if len(sys.argv) != 3:
        print("usage: compare_jsonl.py EXPECTED.jsonl ACTUAL.jsonl", file=sys.stderr)
        return 2
    exp_path, act_path = sys.argv[1], sys.argv[2]
    exp = load(exp_path)
    act = load(act_path)
    if exp == act:
        return 0
    print(f"MISMATCH expected={exp_path} actual={act_path}", file=sys.stderr)
    for r, n in (exp - act).items():
        print(f"  -{n} expected-only: {r}", file=sys.stderr)
    for r, n in (act - exp).items():
        print(f"  +{n} actual-only:   {r}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
