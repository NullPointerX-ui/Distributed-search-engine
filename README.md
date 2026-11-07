# Distributed Search Engine

Mini-Google on 4 VMs: ZeroTier + HDFS + Spark + Apache Iceberg + FastAPI.

| Node | Role | Owner |
|---|---|---|
| node1 | Ingestion (Spark driver) | P1 |
| node2 | HDFS NameNode + DataNode, Spark worker, Iceberg tables | P2 |
| node3 | Spark master + worker, HDFS DataNode, indexing / PageRank / export | P3 |
| node4 | FastAPI search service + UI (BM25 + PageRank) | P4 |

Setup and run steps: see [RUNBOOK.md](RUNBOOK.md).
