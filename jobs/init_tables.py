"""Create the Iceberg namespace + tables on HDFS. Safe to re-run (IF NOT EXISTS).

Submit from Node 2 (it 'maintains the Iceberg tables'):
    bash scripts/submit.sh jobs/init_tables.py
"""
from common import *

DDL = [
    f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}",
    f"""CREATE TABLE IF NOT EXISTS {T_PAGES} (
          doc_id STRING NOT NULL, url STRING, title STRING, text STRING,
          links ARRAY<STRING>, version BIGINT, phase INT, ingested_at TIMESTAMP)
        USING iceberg PARTITIONED BY (bucket(4, doc_id))
        TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='copy-on-write',
                       'write.delete.mode'='copy-on-write', 'write.update.mode'='copy-on-write')""",
    f"""CREATE TABLE IF NOT EXISTS {T_LINK_RECORDS} (src STRING, dst STRING, phase INT)
        USING iceberg TBLPROPERTIES ('format-version'='2')""",
    f"""CREATE TABLE IF NOT EXISTS {T_LINKS} (src STRING, dst STRING)
        USING iceberg TBLPROPERTIES ('format-version'='2')""",
    f"""CREATE TABLE IF NOT EXISTS {T_REJECTS} (
          phase INT, line_no BIGINT, reason STRING, raw STRING, rejected_at TIMESTAMP)
        USING iceberg PARTITIONED BY (phase) TBLPROPERTIES ('format-version'='2')""",
    f"""CREATE TABLE IF NOT EXISTS {T_INGEST_LOG} (
          phase INT, input_path STRING, lines_read BIGINT, accepted BIGINT, rejected BIGINT,
          duplicates_dropped BIGINT, inserted BIGINT, updated BIGINT, deleted BIGINT,
          unchanged_or_stale BIGINT, links_total BIGINT, dangling_links_removed BIGINT,
          pages_snapshot BIGINT, ingested_at TIMESTAMP)
        USING iceberg TBLPROPERTIES ('format-version'='2')""",
    f"""CREATE TABLE IF NOT EXISTS {T_POSTINGS} (term STRING, doc_id STRING, tf INT)
        USING iceberg PARTITIONED BY (bucket(8, term)) TBLPROPERTIES ('format-version'='2')""",
    f"""CREATE TABLE IF NOT EXISTS {T_DOC_STATS} (doc_id STRING, doc_len INT)
        USING iceberg TBLPROPERTIES ('format-version'='2')""",
    f"""CREATE TABLE IF NOT EXISTS {T_PAGERANK} (doc_id STRING, rank DOUBLE)
        USING iceberg TBLPROPERTIES ('format-version'='2')""",
    f"""CREATE TABLE IF NOT EXISTS {T_JOB_STATE} (job STRING, snapshots STRING, updated_at TIMESTAMP)
        USING iceberg TBLPROPERTIES ('format-version'='2')""",
]

if __name__ == "__main__":
    spark = get_spark("init-tables")
    for stmt in DDL:
        print(stmt.split("(")[0].strip())
        spark.sql(stmt)
    spark.sql(f"SHOW TABLES IN {NAMESPACE}").show(truncate=False)
    spark.stop()
