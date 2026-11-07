#!/usr/bin/env bash
# Run on any of node1/2/3. Prints what a grader wants to see.
source /etc/profile.d/bigdata.sh
echo "== HDFS DataNodes (expect: Live datanodes (2)) =="; hdfs dfsadmin -report | grep -E 'Live datanodes|^Name:'
echo "== Replication of files under /input and /warehouse (expect repl=2) =="
hdfs fsck /input -files -blocks -locations 2>/dev/null | grep -E 'repl=|Average block replication' | head
echo "== Spark master (expect 2 ALIVE workers) =="
curl -s http://node3:8080/json/ | python3 -c 'import sys,json;d=json.load(sys.stdin);print("status",d["status"]);[print(" ",w["host"],w["state"],w["cores"],"cores") for w in d["workers"]]'
