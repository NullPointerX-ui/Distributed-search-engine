#!/usr/bin/env bash
# Usage: bash scripts/submit.sh jobs/<job>.py [job args...]
# Submits to the Spark master on node3 in client mode: the DRIVER runs on the
# machine you type this on (node1 for ingestion, node3 for index/pagerank/export).
set -euo pipefail
source /etc/profile.d/bigdata.sh
cd "$(dirname "$0")/.."
JOB="$1"; shift
mkdir -p logs
LOG="logs/$(basename "$JOB" .py)_$(date +%Y%m%d_%H%M%S).log"
echo ">> spark-submit $JOB $*   (log: $LOG)"
spark-submit \
  --master spark://node3:7077 \
  --deploy-mode client \
  --py-files jobs/common.py,jobs/textutil.py \
  "$JOB" "$@" 2>&1 | tee "$LOG"
exit "${PIPESTATUS[0]}"
