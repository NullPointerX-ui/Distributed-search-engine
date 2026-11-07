"""Node 1 — Ingestion (Phase 0 and Phase 1).

    bash scripts/submit.sh jobs/ingest.py --phase 0 --input hdfs://node2:9000/input/pages.jsonl
    bash scripts/submit.sh jobs/ingest.py --phase 1 --input hdfs://node2:9000/input/changes.jsonl

What it does, in order:
  1. Parse every line as JSON; reject malformed lines / missing required fields (with a reason).
  2. Deduplicate by doc_id inside the batch: highest version wins (ties: last line wins).
  3. Compare against the current Iceberg `pages` table and keep only rows that really change
     something (new doc, newer version, or delete). Re-running the same phase -> 0 changes,
     so the MERGE is skipped and table contents stay identical (idempotent).
  4. MERGE INTO pages  (one atomic Iceberg commit = one snapshot = one version of the corpus).
  5. Rebuild the clean link graph: drop self-links and links whose target doc doesn't exist.
  6. Append an audit row to ingest_log with the counts and the new pages snapshot id.

>>> FIRST run `head -3 pages.jsonl` and `head -3 changes.jsonl` and check FIELD_ALIASES below. <<<
"""
import argparse
import json

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Field names we accept for each logical field (first one present wins).
# If your files use other names, add them here — that's the only change needed.
FIELD_ALIASES = {
    "doc_id":  ["doc_id", "id", "page_id", "docid", "pageid"],
    "url":     ["url", "link", "href"],
    "title":   ["title", "name"],
    "text":    ["text", "content", "body"],
    "version": ["version", "ver", "revision", "rev"],
    "links":   ["links", "outlinks", "out_links", "outgoing_links"],
    "op":      ["op", "action", "operation", "change_type", "change"],
    # standalone link records like {"src": "A", "dst": "B"}
    "src":     ["src", "source", "from"],
    "dst":     ["dst", "target", "to", "dest"],
}
DELETE_OPS = {"delete", "del", "remove", "removed", "deleted", "tombstone"}
UPSERT_OPS = {"upsert", "insert", "update", "add", "create", "modify", "put", "new", "edit", ""}

PARSED_SCHEMA = T.StructType([
    T.StructField("line_no", T.LongType()),
    T.StructField("ok", T.BooleanType()),
    T.StructField("kind", T.StringType()),       # page | link
    T.StructField("reason", T.StringType()),
    T.StructField("raw", T.StringType()),
    T.StructField("doc_id", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("links", T.ArrayType(T.StringType())),
    T.StructField("version", T.LongType()),
    T.StructField("op", T.StringType()),         # upsert | delete
])


def _pick(rec, logical):
    for k in FIELD_ALIASES[logical]:
        if k in rec:
            return rec[k]
    return None


def _as_id(v):
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, str)):
        s = str(v).strip()
        return s or None
    if isinstance(v, dict):  # e.g. {"target": "X"} or {"doc_id": "X"}
        for k in ("doc_id", "id", "target", "dst", "to"):
            if k in v:
                return _as_id(v[k])
    return None


def _as_version(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v if v >= 0 else None
    if isinstance(v, float) and v.is_integer() and v >= 0:
        return int(v)
    if isinstance(v, str) and v.strip().isdigit():
        return int(v.strip())
    return None


def parse_line(line_no, line):
    """Pure Python -> tuple matching PARSED_SCHEMA. Runs on the executors."""
    raw = line if len(line) <= 2000 else line[:2000] + "...<truncated>"

    def bad(reason):
        return (line_no, False, None, reason, raw, None, None, None, None, None, None, None)

    if not line.strip():
        return bad("empty_line")
    try:
        rec = json.loads(line)
    except Exception:
        return bad("malformed_json")
    if not isinstance(rec, dict):
        return bad("not_a_json_object")

    op_raw = _pick(rec, "op")
    op_s = str(op_raw).strip().lower() if op_raw is not None else ""
    if rec.get("deleted") is True:
        op_s = "delete"
    if op_s in DELETE_OPS:
        op = "delete"
    elif op_s in UPSERT_OPS:
        op = "upsert"
    else:
        return bad(f"unknown_op:{op_s[:30]}")

    doc_id = _as_id(_pick(rec, "doc_id"))

    # Standalone link record (no doc_id, but src+dst)
    if doc_id is None and _pick(rec, "src") is not None and _pick(rec, "dst") is not None:
        src, dst = _as_id(_pick(rec, "src")), _as_id(_pick(rec, "dst"))
        if not src or not dst:
            return bad("link_missing_src_or_dst")
        return (line_no, True, "link", None, raw, src, None, None, None, [dst], None, op)

    if doc_id is None:
        return bad("missing_doc_id")
    version = _as_version(_pick(rec, "version"))
    if version is None:
        return bad("missing_or_invalid_version")

    if op == "delete":  # a delete only needs id + version
        return (line_no, True, "page", None, raw, doc_id, None, None, None, None, version, op)

    url, title, text = _pick(rec, "url"), _pick(rec, "title"), _pick(rec, "text")
    for name, val in (("url", url), ("title", title), ("text", text)):
        if not isinstance(val, str) or not val.strip():
            return bad(f"missing_{name}")
    if not url.strip().lower().startswith(("http://", "https://")):
        return bad("invalid_url")

    links_v = _pick(rec, "links")
    if links_v is None:
        links = []
    elif isinstance(links_v, list):
        links = sorted({l for l in (_as_id(x) for x in links_v) if l and l != doc_id})
    else:
        return bad("links_not_a_list")

    return (line_no, True, "page", None, raw, doc_id, url.strip(), title.strip(),
            text, links, version, op)


def parse_input(spark, path):
    lines = spark.sparkContext.textFile(path)
    parsed = lines.zipWithIndex().map(lambda li: parse_line(li[1] + 1, li[0]))
    return spark.createDataFrame(parsed, PARSED_SCHEMA)


def dedup_latest(pages_df):
    """Highest version wins per doc_id; tie -> the later line in the file wins."""
    w = Window.partitionBy("doc_id").orderBy(F.col("version").desc(), F.col("line_no").desc())
    return (pages_df.withColumn("_rn", F.row_number().over(w))
            .filter("_rn = 1").drop("_rn"))


def classify_against_target(batch, target):
    """Attach `action` = insert | update | delete | noop by comparing to current table rows."""
    t = target.select(F.col("doc_id").alias("t_id"), F.col("version").alias("t_version"))
    j = batch.join(t, batch.doc_id == t.t_id, "left")
    exists = F.col("t_id").isNotNull()
    action = (F.when(~exists & (F.col("op") == "upsert"), "insert")
              .when(exists & (F.col("op") == "upsert") & (F.col("version") > F.col("t_version")), "update")
              .when(exists & (F.col("op") == "delete") & (F.col("version") >= F.col("t_version")), "delete")
              .otherwise("noop"))
    return j.withColumn("action", action).drop("t_id", "t_version")


def build_link_graph(pages, link_records):
    """(src, dst) pairs where both docs exist; no self-links; no duplicates."""
    from_pages = pages.select("doc_id", F.explode("links").alias("dst")).withColumnRenamed("doc_id", "src")
    edges = from_pages.unionByName(link_records.select("src", "dst")).filter("src <> dst").distinct()
    ids = pages.select("doc_id")
    kept = (edges.join(ids.withColumnRenamed("doc_id", "src"), "src", "left_semi")
                 .join(ids.withColumnRenamed("doc_id", "dst"), "dst", "left_semi")
                 .select("src", "dst"))
    return edges, kept


def main():
    from common import (T_INGEST_LOG, T_LINK_RECORDS, T_LINKS, T_PAGES, T_REJECTS,
                        banner, current_snapshot, get_spark, timed)

    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", type=int, required=True)
    ap.add_argument("--input", required=True)
    args = ap.parse_args()

    spark = get_spark(f"ingest-phase{args.phase}")
    banner(f"INGEST phase={args.phase} input={args.input}")
    before_snap = current_snapshot(spark, T_PAGES)

    with timed("parse + validate"):
        parsed = parse_input(spark, args.input).cache()
        n_read = parsed.count()
        rejects = parsed.filter(~F.col("ok"))
        n_rej = rejects.count()
    print(f"lines read={n_read}  rejected={n_rej}")
    rejects.groupBy("reason").count().orderBy(F.desc("count")).show(truncate=False)

    # Rejects are partitioned by phase; overwritePartitions makes re-runs idempotent.
    (rejects.select(F.lit(args.phase).alias("phase"), "line_no", "reason", "raw",
                    F.current_timestamp().alias("rejected_at"))
            .writeTo(T_REJECTS).overwritePartitions())

    good = parsed.filter("ok")
    page_rows = good.filter("kind = 'page'")
    n_acc = good.count()

    with timed("dedup + diff against Iceberg"):
        latest = dedup_latest(page_rows).cache()
        n_dupes = page_rows.count() - latest.count()
        classified = classify_against_target(latest, spark.table(T_PAGES)).cache()
        counts = {r["action"]: r["count"] for r in classified.groupBy("action").count().collect()}
    print(f"duplicates dropped={n_dupes}  actions={counts}")

    changes = classified.filter("action <> 'noop'")
    n_changes = sum(v for k, v in counts.items() if k != "noop")
    if n_changes == 0:
        print("No page changes -> skipping MERGE (pages table unchanged, no new snapshot).")
    else:
        with timed("MERGE INTO pages"):
            (changes.withColumn("phase", F.lit(args.phase))
                    .withColumn("ingested_at", F.current_timestamp())
                    .createOrReplaceTempView("staged"))
            spark.sql(f"""
                MERGE INTO {T_PAGES} t
                USING staged s
                ON t.doc_id = s.doc_id
                WHEN MATCHED AND s.action = 'delete' THEN DELETE
                WHEN MATCHED AND s.action = 'update' THEN UPDATE SET
                    t.url = s.url, t.title = s.title, t.text = s.text, t.links = s.links,
                    t.version = s.version, t.phase = s.phase, t.ingested_at = s.ingested_at
                WHEN NOT MATCHED AND s.action = 'insert' THEN INSERT
                    (doc_id, url, title, text, links, version, phase, ingested_at)
                    VALUES (s.doc_id, s.url, s.title, s.text, s.links, s.version, s.phase, s.ingested_at)
            """)

    # Standalone link records (only if your dataset has them): insert-if-absent / delete.
    link_rows = good.filter("kind = 'link'").select(
        F.col("doc_id").alias("src"), F.col("links")[0].alias("dst"), "op").distinct()
    if link_rows.limit(1).count() > 0:
        link_rows.createOrReplaceTempView("staged_links")
        spark.sql(f"""
            MERGE INTO {T_LINK_RECORDS} t USING staged_links s
            ON t.src = s.src AND t.dst = s.dst
            WHEN MATCHED AND s.op = 'delete' THEN DELETE
            WHEN NOT MATCHED AND s.op = 'upsert' THEN INSERT (src, dst, phase) VALUES (s.src, s.dst, {args.phase})
        """)

    with timed("rebuild clean link graph"):
        pages_now = spark.table(T_PAGES)
        all_edges, kept = build_link_graph(pages_now, spark.table(T_LINK_RECORDS))
        kept = kept.cache()
        n_edges_all, n_kept = all_edges.count(), kept.count()
        existing = spark.table(T_LINKS)
        differs = (kept.exceptAll(existing).limit(1).count() > 0 or
                   existing.exceptAll(kept).limit(1).count() > 0)
        if differs:
            kept.writeTo(T_LINKS).overwrite(F.lit(True))
        else:
            print("Link graph unchanged -> no new links snapshot.")
    print(f"links: candidate={n_edges_all} kept={n_kept} dangling/removed={n_edges_all - n_kept}")

    after_snap = current_snapshot(spark, T_PAGES)
    log = spark.createDataFrame([(
        args.phase, args.input, n_read, n_acc, n_rej, n_dupes,
        counts.get("insert", 0), counts.get("update", 0), counts.get("delete", 0), counts.get("noop", 0),
        n_kept, n_edges_all - n_kept, after_snap)],
        "phase INT, input_path STRING, lines_read BIGINT, accepted BIGINT, rejected BIGINT, "
        "duplicates_dropped BIGINT, inserted BIGINT, updated BIGINT, deleted BIGINT, "
        "unchanged_or_stale BIGINT, links_total BIGINT, dangling_links_removed BIGINT, pages_snapshot BIGINT")
    log.withColumn("ingested_at", F.current_timestamp()).writeTo(T_INGEST_LOG).append()

    banner(f"DONE. pages snapshot {before_snap} -> {after_snap}. "
           f"pages now={spark.table(T_PAGES).count()} links={n_kept}")
    spark.table(T_INGEST_LOG).orderBy("ingested_at").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
