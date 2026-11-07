#!/usr/bin/env bash
# Run on NODE 3 after ingestion. Stops at the first failure, so a broken stage never exports.
set -euo pipefail
cd "$(dirname "$0")/.."
bash scripts/submit.sh jobs/index.py
bash scripts/submit.sh jobs/pagerank.py --iterations "${ITERATIONS:-20}"
bash scripts/submit.sh jobs/export.py
source /etc/profile.d/bigdata.sh; hdfs dfs -ls /exports
