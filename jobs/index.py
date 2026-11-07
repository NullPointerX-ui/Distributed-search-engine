"""Nodes 2 & 3 — Build / incrementally update the distributed inverted index.

Driver runs on Node 3; tasks run on the workers on Nodes 2 and 3.
    bash scripts/submit.sh jobs/index.py            # incremental (full build on first run)
    bash scripts/submit.sh jobs/index.py --full     # force a full rebuild

Incremental logic (uses Iceberg time travel):
  - job_state remembers the pages snapshot the index was last built from.
  - Compare (doc_id, version) at that snapshot vs now  ->  changed doc_ids.
  - Drop postings of changed docs, re-tokenize only those docs, write the result.
  - Nothing changed? -> exits without writing (idempotent).
"""
import argparse
from collections import Counter

from pyspark.sql import functions as F
from pyspark.sql import types as T

POSTING_SCHEMA = T.StructType([
    T.StructField("term", T.StringType()),
    T.StructField("doc_id", T.StringType()),
    T.StructField("tf", T.IntegerType()),
])
STATS_SCHEMA = T.StructType([
    T.StructField("doc_id", T.StringType()),
    T.StructField("doc_len", T.IntegerType()),
])


def doc_tokens(title, text):
    from textutil import tokenize   # imported on the executor
    # Title counts once more than body text: a cheap, common relevance boost.
    return tokenize(title or "") + tokenize(title or "") + tokenize(text or "")


def build_postings(spark, pages_df):
    """pages(doc_id,title,text) -> (postings DF, doc_stats DF). Map: tokenize; reduce: count."""
    rdd = pages_df.select("doc_id", "title", "text").rdd.map(
        lambda r: (r.doc_id, doc_tokens(r.title, r.text))).cache()
    postings = rdd.flatMap(lambda kv: [(t, kv[0], c) for t, c in Counter(kv[1]).items()])
    stats = rdd.map(lambda kv: (kv[0], len(kv[1])))
    return (spark.createDataFrame(postings, POSTING_SCHEMA),
            spark.createDataFrame(stats, STATS_SCHEMA))


def changed_doc_ids(old_df, new_df):
    """doc_ids whose (exists, version) differ between two snapshots of pages."""
    o = old_df.select("doc_id", F.col("version").alias("v_old"))
    n = new_df.select("doc_id", F.col("version").alias("v_new"))
    j = o.join(n, "doc_id", "full_outer")
    return (j.filter(~F.col("v_old").eqNullSafe(F.col("v_new")))
             .withColumn("change", F.when(F.col("v_old").isNull(), "inserted")
                                   .when(F.col("v_new").isNull(), "deleted")
                                   .otherwise("updated"))
             .select("doc_id", "change"))


def main():
    from common import (T_DOC_STATS, T_PAGES, T_POSTINGS, banner, current_snapshot,
                        get_spark, get_state, set_state, state_snapshots, timed)

    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true")
    args = ap.parse_args()

    spark = get_spark("build-inverted-index")
    cur = current_snapshot(spark, T_PAGES)
    prev = state_snapshots(get_state(spark, "index")).get("pages_snapshot")
    banner(f"INDEX  pages snapshot: last indexed={prev}  current={cur}")

    if cur is None:
        raise SystemExit("pages table is empty - run ingestion first")
    if prev == cur and not args.full:
        print("Index already up to date with current pages snapshot. Nothing to do.")
        return

    pages = spark.table(T_PAGES)
    mode = "full"
    if prev is not None and not args.full:
        try:
            old = spark.sql(f"SELECT doc_id, version FROM {T_PAGES} VERSION AS OF {prev}")
            changed = changed_doc_ids(old, pages).cache()
            changed.groupBy("change").count().show()
            mode = "incremental"
        except Exception as e:  # snapshot expired, etc.
            print(f"Time travel to {prev} failed ({e}); falling back to full rebuild")

    with timed(f"{mode} index build"):
        if mode == "full":
            postings, stats = build_postings(spark, pages)
        else:
            ids = changed.select("doc_id")
            to_index = pages.join(ids, "doc_id", "left_semi")
            new_p, new_s = build_postings(spark, to_index)
            # keep postings of unchanged docs, replace the rest
            postings = spark.table(T_POSTINGS).join(ids, "doc_id", "left_anti").unionByName(new_p)
            stats = spark.table(T_DOC_STATS).join(ids, "doc_id", "left_anti").unionByName(new_s)
            n_del = changed.filter("change = 'deleted'").count()
            print(f"re-tokenized {to_index.count()} docs; removed {n_del} deleted docs")

        # materialize before overwriting the tables we may have read from
        postings = postings.select("term", "doc_id", "tf").repartition(8, "term").cache()
        stats = stats.select("doc_id", "doc_len").cache()
        n_post, n_docs = postings.count(), stats.count()
        postings.writeTo(T_POSTINGS).overwrite(F.lit(True))
        stats.writeTo(T_DOC_STATS).overwrite(F.lit(True))

    set_state(spark, "index", pages_snapshot=cur)
    n_terms = spark.table(T_POSTINGS).select("term").distinct().count()
    banner(f"INDEX DONE ({mode}): docs={n_docs} postings={n_post} distinct terms={n_terms}")
    spark.table(T_POSTINGS).groupBy("term").agg(F.count("*").alias("df")) \
        .orderBy(F.desc("df")).show(10)
    spark.stop()


if __name__ == "__main__":
    main()
