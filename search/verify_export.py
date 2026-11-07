"""Check a pulled export directory before it is allowed to go live. Exit 0 = OK."""
import json, sys
from pathlib import Path
import pyarrow.parquet as pq

d = Path(sys.argv[1])
m = json.loads((d / "manifest.json").read_text())
assert m.get("status") == "complete", "manifest not complete"
nd = pq.read_table(d / "docs").num_rows
npost = pq.read_table(d / "postings").num_rows
assert nd == m["num_docs"], f"docs {nd} != manifest {m['num_docs']}"
assert npost == m["num_postings"], f"postings {npost} != manifest {m['num_postings']}"
assert nd > 0 and npost > 0, "empty export"
print(f"OK v{m['version']}: {nd} docs, {npost} postings, avgdl={m['avgdl']:.1f}")
