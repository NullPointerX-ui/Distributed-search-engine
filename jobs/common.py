"""Shared settings + helpers for every Spark job. Shipped to executors with --py-files."""
import json
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession

CATALOG = "lake"
NAMESPACE = f"{CATALOG}.db"

T_PAGES = f"{NAMESPACE}.pages"            # latest version of each document
T_LINK_RECORDS = f"{NAMESPACE}.link_records"  # standalone link records (if the dataset has them)
T_LINKS = f"{NAMESPACE}.links"            # clean link graph: (src, dst), both exist
T_REJECTS = f"{NAMESPACE}.rejects"        # malformed / invalid input lines, with reason
T_INGEST_LOG = f"{NAMESPACE}.ingest_log"  # one audit row per ingestion run
T_POSTINGS = f"{NAMESPACE}.postings"      # inverted index: (term, doc_id, tf)
T_DOC_STATS = f"{NAMESPACE}.doc_stats"    # (doc_id, doc_len) for BM25
T_PAGERANK = f"{NAMESPACE}.pagerank"      # (doc_id, rank)
T_JOB_STATE = f"{NAMESPACE}.job_state"    # which source snapshot each job last processed

EXPORT_ROOT = "hdfs://node2:9000/exports"
CHECKPOINT_DIR = "hdfs://node2:9000/checkpoints"

DAMPING = 0.85


def get_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def current_snapshot(spark, table):
    """Snapshot id the table's main branch points at (None if table is empty/new)."""
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()
    return int(rows[0][0]) if rows else None


def get_state(spark, job):
    rows = spark.sql(f"SELECT * FROM {T_JOB_STATE} WHERE job = '{job}'").collect()
    return rows[0].asDict() if rows else None


def set_state(spark, job, **snapshots):
    """Replace the state row for `job`. snapshots: e.g. pages_snapshot=123."""
    payload = json.dumps(snapshots)
    spark.sql(f"DELETE FROM {T_JOB_STATE} WHERE job = '{job}'")
    spark.sql(f"INSERT INTO {T_JOB_STATE} VALUES ('{job}', '{payload}', current_timestamp())")


def state_snapshots(state):
    return json.loads(state["snapshots"]) if state else {}


@contextmanager
def timed(label):
    t0 = time.time()
    yield
    print(f"[time] {label}: {time.time() - t0:.1f}s", flush=True)


def banner(msg):
    print("\n" + "=" * 70 + f"\n{msg}\n" + "=" * 70, flush=True)
