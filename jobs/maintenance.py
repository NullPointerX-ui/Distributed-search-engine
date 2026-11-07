"""Node 2 — Iceberg table maintenance, time travel and rollback (for the demo/viva).

    bash scripts/submit.sh jobs/maintenance.py snapshots                 # history of every table
    bash scripts/submit.sh jobs/maintenance.py diff --from S1 --to S2    # what changed in pages
    bash scripts/submit.sh jobs/maintenance.py rollback --table pages --snapshot S1
    bash scripts/submit.sh jobs/maintenance.py compact                   # rewrite small files
    bash scripts/submit.sh jobs/maintenance.py rejects                   # what got rejected and why
"""
import argparse

from pyspark.sql import functions as F

from common import (NAMESPACE, T_INGEST_LOG, T_PAGES, T_REJECTS, banner, get_spark)

TABLES = ["pages", "links", "postings", "doc_stats", "pagerank"]


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("snapshots")
    d = sub.add_parser("diff"); d.add_argument("--from", dest="s1", required=True); d.add_argument("--to", dest="s2", required=True)
    r = sub.add_parser("rollback"); r.add_argument("--table", required=True); r.add_argument("--snapshot", required=True)
    sub.add_parser("compact")
    sub.add_parser("rejects")
    args = ap.parse_args()
    spark = get_spark(f"maintenance-{args.cmd}")

    if args.cmd == "snapshots":
        for t in TABLES:
            banner(f"{NAMESPACE}.{t}")
            spark.sql(f"""SELECT s.committed_at, s.snapshot_id, s.parent_id, s.operation,
                                 s.summary['added-records'] AS added, s.summary['deleted-records'] AS deleted,
                                 s.summary['total-records'] AS total, h.is_current_ancestor AS current
                          FROM {NAMESPACE}.{t}.snapshots s JOIN {NAMESPACE}.{t}.history h
                            ON s.snapshot_id = h.snapshot_id ORDER BY s.committed_at""").show(truncate=False)
        banner("ingest_log"); spark.table(T_INGEST_LOG).orderBy("ingested_at").show(truncate=False)

    elif args.cmd == "diff":
        a = spark.sql(f"SELECT doc_id, version, title FROM {T_PAGES} VERSION AS OF {args.s1}")
        b = spark.sql(f"SELECT doc_id, version, title FROM {T_PAGES} VERSION AS OF {args.s2}")
        j = a.alias("a").join(b.alias("b"), "doc_id", "full_outer")
        out = j.select("doc_id", F.col("a.version").alias("v_from"), F.col("b.version").alias("v_to"),
                       F.coalesce("b.title", "a.title").alias("title")) \
               .filter(~F.col("v_from").eqNullSafe(F.col("v_to")))
        out.groupBy(F.when(F.col("v_from").isNull(), "inserted")
                     .when(F.col("v_to").isNull(), "deleted").otherwise("updated").alias("change")).count().show()
        out.show(50, truncate=60)

    elif args.cmd == "rollback":
        spark.sql(f"CALL lake.system.rollback_to_snapshot('db.{args.table}', {args.snapshot})").show()
        print("Now re-run index.py / pagerank.py / export.py so serving matches the rolled-back data.")

    elif args.cmd == "compact":
        for t in TABLES:
            spark.sql(f"CALL lake.system.rewrite_data_files(table => 'db.{t}')").show()

    elif args.cmd == "rejects":
        spark.table(T_REJECTS).groupBy("phase", "reason").count().orderBy("phase", F.desc("count")).show(50, truncate=False)
        spark.table(T_REJECTS).orderBy("phase", "line_no").show(20, truncate=100)
    spark.stop()


if __name__ == "__main__":
    main()
