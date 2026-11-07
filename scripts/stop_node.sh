#!/usr/bin/env bash
set -uo pipefail; source /etc/profile.d/bigdata.sh
stop-worker.sh; stop-master.sh 2>/dev/null; stop-history-server.sh 2>/dev/null
hdfs --daemon stop datanode; hdfs --daemon stop namenode 2>/dev/null
jps
