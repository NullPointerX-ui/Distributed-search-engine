#!/usr/bin/env bash
# Run on NODE 1.   bash scripts/node1_ingest_phase.sh 0 ~/data/pages.jsonl
#                  bash scripts/node1_ingest_phase.sh 1 ~/data/changes.jsonl
set -euo pipefail
source /etc/profile.d/bigdata.sh
PHASE="$1"; FILE="$2"; NAME="$(basename "$FILE")"
hdfs dfs -mkdir -p /input
hdfs dfs -put -f "$FILE" "/input/$NAME"
hdfs dfs -setrep -w 2 "/input/$NAME"         # make sure both DataNodes hold a copy
hdfs fsck "/input/$NAME" -files -blocks -locations | grep -E 'repl=|Status'
bash "$(dirname "$0")/submit.sh" jobs/ingest.py --phase "$PHASE" --input "hdfs://node2:9000/input/$NAME"
