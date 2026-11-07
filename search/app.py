"""Node 4 — FastAPI search service: BM25 (k1=1.2, b=0.75) + PageRank (d=0.85, weight 0.3).

Run:   /opt/searchenv/bin/uvicorn app:app --host 0.0.0.0 --port 8000   (from the search/ dir)

Serves whatever /serving/current points to (a symlink to /serving/vN). Never touches
Spark, HDFS or Iceberg: it only reads the plain Parquet export that pull_export.sh copied.
Reload is all-or-nothing: if the new version fails to load or validate, the old one
keeps serving and /admin/reload returns 500.
"""
import json
import math
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from textutil import tokenize

SERVING_ROOT = Path(os.environ.get("SERVING_ROOT", "/serving"))
K1 = 1.2
B = 0.75
W_BM25 = 0.7
W_PR = 0.3
# "results": divide by the max among the matching docs (default)
# "global":  divide PageRank by the max over the whole corpus
PR_NORM = os.environ.get("PR_NORM", "results")


class Index:
    """Immutable in-memory copy of one export version."""

    def __init__(self, path: Path):
        t0 = time.time()
        self.path = path.resolve()
        self.manifest = json.loads((self.path / "manifest.json").read_text())
        if self.manifest.get("status") != "complete":
            raise ValueError("manifest status is not 'complete'")

        docs = pq.read_table(self.path / "docs").to_pandas()
        posts = pq.read_table(self.path / "postings").to_pandas()
        if len(docs) != self.manifest["num_docs"] or len(posts) != self.manifest["num_postings"]:
            raise ValueError(f"row counts {len(docs)}/{len(posts)} don't match manifest")

        docs = docs.sort_values("doc_id").reset_index(drop=True)
        self.doc_id = docs["doc_id"].astype(str).to_numpy()
        self.url = docs["url"].astype(str).to_numpy()
        self.title = docs["title"].fillna("").astype(str).to_numpy()
        self.doc_len = docs["doc_len"].fillna(0).to_numpy(dtype=np.float64)
        self.pagerank = docs["pagerank"].to_numpy(dtype=np.float64)
        self.N = len(docs)
        self.avgdl = float(self.doc_len.mean()) if self.N else 0.0
        self.max_pr_global = float(self.pagerank.max()) if self.N else 0.0
        # precompute the BM25 length-normalization denominator part per doc
        self.len_norm = K1 * (1 - B + B * self.doc_len / (self.avgdl or 1.0))

        pos = {d: i for i, d in enumerate(self.doc_id)}
        posts["di"] = posts["doc_id"].astype(str).map(pos)
        if posts["di"].isna().any():
            raise ValueError("postings reference unknown doc_ids")
        posts = posts.sort_values("term", kind="stable")
        terms = posts["term"].to_numpy()
        di = posts["di"].to_numpy(dtype=np.int64)
        tf = posts["tf"].to_numpy(dtype=np.float64)
        uniq, starts = np.unique(terms, return_index=True)
        ends = np.append(starts[1:], len(terms))
        # term -> (doc indices, term frequencies)   == the inverted index
        self.postings = {t: (di[s:e], tf[s:e]) for t, s, e in zip(uniq, starts, ends)}
        self.load_seconds = round(time.time() - t0, 3)

    def idf(self, df):
        # Lucene/Okapi BM25 IDF (always positive)
        return math.log(1 + (self.N - df + 0.5) / (df + 0.5))

    def search(self, query, k=10):
        terms = list(dict.fromkeys(tokenize(query)))   # unique, keep order
        bm25 = np.zeros(self.N)
        matched_terms = []
        for t in terms:
            if t not in self.postings:
                continue
            di, tf = self.postings[t]
            matched_terms.append(t)
            bm25[di] += self.idf(len(di)) * tf * (K1 + 1) / (tf + self.len_norm[di])

        cand = np.nonzero(bm25 > 0)[0]
        if cand.size == 0:
            return terms, matched_terms, 0, []
        bm = bm25[cand]
        pr = self.pagerank[cand]
        bm_norm = bm / bm.max()
        pr_max = self.max_pr_global if PR_NORM == "global" else pr.max()
        pr_norm = pr / pr_max if pr_max > 0 else np.zeros_like(pr)
        final = W_BM25 * bm_norm + W_PR * pr_norm

        order = np.lexsort((self.doc_id[cand], -final))[:k]   # score desc, doc_id tiebreak
        results = []
        for rank, j in enumerate(order, 1):
            i = cand[j]
            results.append({
                "rank": rank, "doc_id": self.doc_id[i], "title": self.title[i], "url": self.url[i],
                "score": round(float(final[j]), 6),
                "bm25": round(float(bm[j]), 6), "bm25_norm": round(float(bm_norm[j]), 6),
                "pagerank": float(pr[j]), "pagerank_norm": round(float(pr_norm[j]), 6),
            })
        return terms, matched_terms, int(cand.size), results


_lock = threading.Lock()
_index = None
_last_error = None


def load_current():
    """Load SERVING_ROOT/current; swap in only on success."""
    global _index, _last_error
    target = SERVING_ROOT / "current"
    try:
        new = Index(target)
    except Exception as e:
        _last_error = f"{type(e).__name__}: {e}"
        raise
    with _lock:
        _index = new
        _last_error = None
    return new


@asynccontextmanager
async def lifespan(_app):
    try:
        idx = load_current()
        print(f"Loaded {idx.path} ({idx.N} docs) in {idx.load_seconds}s")
    except Exception as e:
        print(f"WARNING: no index loaded yet: {e}")
    yield


app = FastAPI(title="Mini-Google", lifespan=lifespan)


def _require():
    if _index is None:
        raise HTTPException(503, f"No index loaded. {_last_error or ''}")
    return _index


@app.get("/search")
def search(q: str = Query(..., min_length=1, max_length=300), k: int = Query(10, ge=1, le=100)):
    idx = _require()
    t0 = time.perf_counter()
    terms, matched, total, results = idx.search(q, k)
    return {"query": q, "terms": terms, "matched_terms": matched, "total_hits": total,
            "took_ms": round((time.perf_counter() - t0) * 1000, 2),
            "index_version": idx.manifest["version"], "results": results}


@app.get("/doc/{doc_id}")
def doc(doc_id: str):
    idx = _require()
    hits = np.nonzero(idx.doc_id == doc_id)[0]
    if not hits.size:
        raise HTTPException(404, "unknown doc_id")
    i = hits[0]
    return {"doc_id": doc_id, "title": idx.title[i], "url": idx.url[i],
            "doc_len": int(idx.doc_len[i]), "pagerank": float(idx.pagerank[i])}


@app.get("/version")
def version():
    idx = _require()
    return {"serving_path": str(idx.path), "load_seconds": idx.load_seconds,
            "config": {"k1": K1, "b": B, "w_bm25": W_BM25, "w_pagerank": W_PR, "pr_norm": PR_NORM},
            "manifest": idx.manifest}


@app.get("/health")
def health():
    return {"ok": _index is not None,
            "version": _index.manifest["version"] if _index else None,
            "last_reload_error": _last_error}


@app.post("/admin/reload")
def reload():
    old = _index.manifest["version"] if _index else None
    try:
        idx = load_current()
    except Exception as e:
        raise HTTPException(500, f"reload failed, still serving v{old}: {e}")
    return {"previous_version": old, "now_serving": idx.manifest["version"], "docs": idx.N}


@app.get("/")
def ui():
    return FileResponse(Path(__file__).parent / "static" / "index.html")
