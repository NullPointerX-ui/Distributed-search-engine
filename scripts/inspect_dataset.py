"""Run this FIRST on Node 1:  python3 scripts/inspect_dataset.py ~/data/pages.jsonl ~/data/changes.jsonl
Shows which JSON keys the files really use, so you can check FIELD_ALIASES in jobs/ingest.py."""
import collections
import json
import sys

for path in sys.argv[1:]:
    keys, types, bad, n = collections.Counter(), collections.Counter(), 0, 0
    first = None
    with open(path, encoding="utf-8") as f:
        for line in f:
            n += 1
            try:
                rec = json.loads(line)
                if not isinstance(rec, dict):
                    bad += 1
                    continue
                first = first or rec
                keys.update(rec.keys())
                for k, v in rec.items():
                    types[(k, type(v).__name__)] += 1
            except Exception:
                bad += 1
    print(f"\n=== {path}: {n} lines, {bad} not parseable as a JSON object")
    for k, c in keys.most_common():
        ts = ", ".join(f"{t}:{c2}" for (k2, t), c2 in types.items() if k2 == k)
        print(f"  {k:<20} present in {c:>6} lines   types: {ts}")
    if first:
        short = {k: (v[:80] + "...") if isinstance(v, str) and len(v) > 80 else v for k, v in first.items()}
        print("  example:", json.dumps(short)[:600])
