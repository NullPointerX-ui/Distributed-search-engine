#!/usr/bin/env bash
# Usage (as CLUSTER_USER, not root): bash scripts/start_node.sh <2|3>
# Start node 2 BEFORE node 3 the first time.
set -euo pipefail
source /etc/profile.d/bigdata.sh
N="${1:?node 2 or 3}"
case "$N" in
  2)
    if [ ! -d /data/hdfs/namenode/current ]; then
      echo "Formatting NameNode (first run only)"; hdfs namenode -format -force -nonInteractive
    fi
    hdfs --daemon start namenode
    hdfs --daemon start datanode
    sleep 5
    hdfs dfs -mkdir -p /warehouse /spark-logs /input /exports /checkpoints
    start-worker.sh spark://node3:7077
    ;;
  3)
    hdfs --daemon start datanode
    start-master.sh
    sleep 3
    start-worker.sh spark://node3:7077
    start-history-server.sh || true
    ;;
  *) echo "Only nodes 2 and 3 run daemons. Nodes 1 and 4 are clients."; exit 1;;
esac
jps
