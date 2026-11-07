"""Nodes 2 & 3 — PageRank (damping 0.85, default 20 iterations, minimum 10).

    bash scripts/submit.sh jobs/pagerank.py --iterations 20

PR(p) = (1-d)/N + d * ( sum_{q->p} PR(q)/out(q)  +  dangling_mass/N )
Dangling pages (no out-links) spread their rank evenly, so ranks always sum to 1.
Lineage is cut with an HDFS checkpoint every 5 iterations (fault tolerance: a lost
executor only recomputes from the last checkpoint, not from iteration 1).
"""
import argparse

from pyspark.sql import functions as F


def pagerank(spark, nodes, edges, iterations=20, d=0.85, checkpoint_every=5, log=print):
    """nodes: DF(doc_id). edges: DF(src, dst) with both ends in nodes. Returns DF(doc_id, rank)."""
    nodes = nodes.select("doc_id").distinct().cache()
    n = nodes.count()
    if n == 0:
        raise ValueError("no nodes")
    edges = edges.select("src", "dst").distinct()
    outdeg = edges.groupBy("src").agg(F.count("*").alias("out"))
    # weight each edge once: w = 1/out(src)
    wedges = edges.join(outdeg, "src").select("src", "dst", (1.0 / F.col("out")).alias("w")).cache()
    dangling = nodes.join(outdeg.withColumnRenamed("src", "doc_id"), "doc_id", "left_anti").cache()
    log(f"N={n} edges={wedges.count()} dangling={dangling.count()}")

    ranks = nodes.withColumn("rank", F.lit(1.0 / n))
    for i in range(1, iterations + 1):
        contribs = (wedges.join(ranks.withColumnRenamed("doc_id", "src"), "src")
                    .groupBy("dst").agg(F.sum(F.col("rank") * F.col("w")).alias("c"))
                    .withColumnRenamed("dst", "doc_id"))
        dmass = dangling.join(ranks, "doc_id").agg(F.sum("rank")).first()[0] or 0.0
        new = (nodes.join(contribs, "doc_id", "left").fillna(0.0, ["c"])
               .select("doc_id", ((1 - d) / n + d * (F.col("c") + dmass / n)).alias("rank")))
        if i % checkpoint_every == 0:
            new = new.checkpoint()      # eager, written to HDFS checkpoint dir
        else:
            new = new.cache()
        delta = (new.join(ranks.withColumnRenamed("rank", "old"), "doc_id")
                 .agg(F.sum(F.abs(F.col("rank") - F.col("old")))).first()[0])
        ranks.unpersist()
        ranks = new
        log(f"iteration {i:2d}: L1 change={delta:.3e}  dangling mass={dmass:.4f}")
    return ranks


def main():
    from common import (CHECKPOINT_DIR, DAMPING, T_LINKS, T_PAGERANK, T_PAGES, banner,
                        current_snapshot, get_spark, set_state, timed)

    ap = argparse.ArgumentParser()
    ap.add_argument("--iterations", type=int, default=20)
    args = ap.parse_args()
    if args.iterations < 10:
        raise SystemExit("Spec requires at least 10 iterations")

    spark = get_spark("pagerank")
    spark.sparkContext.setCheckpointDir(f"{CHECKPOINT_DIR}/pagerank")
    pages_snap, links_snap = current_snapshot(spark, T_PAGES), current_snapshot(spark, T_LINKS)
    banner(f"PAGERANK d={DAMPING} iterations={args.iterations} "
           f"pages_snapshot={pages_snap} links_snapshot={links_snap}")

    with timed("pagerank"):
        ranks = pagerank(spark, spark.table(T_PAGES), spark.table(T_LINKS),
                         iterations=args.iterations, d=DAMPING)
        ranks.writeTo(T_PAGERANK).overwrite(F.lit(True))

    total = spark.table(T_PAGERANK).agg(F.sum("rank")).first()[0]
    set_state(spark, "pagerank", pages_snapshot=pages_snap, links_snapshot=links_snap,
              iterations=args.iterations)
    banner(f"PAGERANK DONE: sum of ranks = {total:.6f} (should be ~1.0)")
    (spark.table(T_PAGERANK).join(spark.table(T_PAGES).select("doc_id", "title"), "doc_id")
         .orderBy(F.desc("rank")).show(10, truncate=60))
    spark.stop()


if __name__ == "__main__":
    main()
