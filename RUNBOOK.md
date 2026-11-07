# Distributed Search Engine — Solo Runbook

From four blank VMs to a ranked search query, one command at a time.

Read this whole section before you type anything.

* **Assumes** Ubuntu 22.04 on all 4 VMs, with at least 2 vCPU, 4 GB RAM and 20 GB disk each, and the same login user on all of them (`ubuntu` by default; change `CLUSTER_USER` in `cluster.env` if yours differs).
* **Versions are pinned:** Java 11, Hadoop 3.3.6, Spark 3.5.3, Iceberg 1.6.1 (`iceberg-spark-runtime-3.5_2.12`), Python 3.10 (Ubuntu's default).
* **Who runs what:**

| Node | Daemons it runs | Spark drivers you launch from it |
|---|---|---|
| node1 | none (HDFS and Spark client only) | `ingest.py` |
| node2 | HDFS NameNode, HDFS DataNode, Spark Worker | `init_tables.py`, `maintenance.py` |
| node3 | Spark Master, Spark Worker, HDFS DataNode, History Server | `index.py`, `pagerank.py`, `export.py` |
| node4 | FastAPI (uvicorn) only, plus an HDFS client to pull exports | none |

* **Every `.sh` script runs from the project root** (the `dse/` folder). Scripts marked *sudo* need root. Everything else runs as your normal user.
* **Tell your instructor today.** Write that your three teammates are in hospital and you're running the project alone. Ask for an extension or a reduced scope. It costs one email and protects your grade.

---

## Phase 1 — Infrastructure

### 1.1 Get the project onto all 4 VMs

On your laptop, unzip `dse.zip`, then:

```bash
# repeat for each VM's public/SSH address
scp -r dse ubuntu@<vm-address>:~/
```

(Or push it to a private GitHub repo and `git clone` it on each VM.)

### 1.2 ZeroTier private network (≈10 min)

1. Go to <https://my.zerotier.com>, sign in, and click **Create A Network**. Copy the 16-character **Network ID**.
2. In the network settings:
   * Leave **Access Control** on **Private**.
   * Under **IPv4 Auto-Assign**, pick a range, e.g. `10.147.17.*`.
3. On **every** VM:

   ```bash
   curl -s https://install.zerotier.com | sudo bash
   sudo zerotier-cli join <NETWORK_ID>
   ```

4. Back in the web UI under **Members**, tick **Auth** for all 4 machines. Give each one a fixed IP in the "Managed IPs" box, e.g. node1 = `.11`, node2 = `.12`, node3 = `.13`, node4 = `.14`. Delete any random IPs it auto-assigned.
5. On each VM, run `ip -4 addr | grep zt`. You should see that VM's `10.147.17.x` address. If you don't, wait 30 s and run `sudo zerotier-cli listnetworks`; the status must be `OK`.
6. On **every** VM, edit `dse/cluster.env` with the 4 real IPs and your subnet (all 4 copies must be identical).
7. On **every** VM, replacing N with 1, 2, 3 or 4:

   ```bash
   cd ~/dse
   sudo bash scripts/setup_hosts.sh N
   ```

   It sets the hostname to `nodeN` and writes `node1..node4` into `/etc/hosts`. It also deletes Ubuntu's `127.0.1.1` line, which is the #1 cause of "DataNode can't reach NameNode". At the end it pings all four nodes, and every one must say "reachable".

### 1.3 Install Java, Hadoop, Spark and Iceberg (≈10 min per node, run all 4 in parallel)

```bash
cd ~/dse
sudo bash scripts/install_node.sh N        # N = this node's number
source /etc/profile.d/bigdata.sh           # or log out and back in
```

What this does:

* Installs OpenJDK 11 and Hadoop 3.3.6 into `/opt/hadoop` on all 4 nodes.
* Installs Spark 3.5.3 into `/opt/spark` and drops the Iceberg runtime jar into `/opt/spark/jars/` on nodes 1–3.
* Copies `conf/core-site.xml`, `conf/hdfs-site.xml` and `conf/workers` to every node.
* Writes `spark-defaults.conf` (Iceberg catalog `lake` → `hdfs://node2:9000/warehouse`) and a per-node `spark-env.sh` (binds to the ZeroTier IP).
* Opens the firewall to the ZeroTier subnet only.
* On node4, also creates the Python venv `/opt/searchenv` with FastAPI, pandas and pyarrow.

The key configuration values:

| Setting | Value | Why |
|---|---|---|
| `fs.defaultFS` | `hdfs://node2:9000` | NameNode lives on node2 |
| `dfs.replication` | `2` | Spec: replicas on node2 + node3 |
| `dfs.*.use.datanode.hostname` | `true` | Forces traffic onto the ZeroTier hostnames |
| `spark.master` | `spark://node3:7077` | Master on node3 |
| `spark.sql.catalog.lake.type` | `hadoop` | Iceberg tables + metadata stored directly in HDFS, no Hive metastore |
| `SPARK_WORKER_CORES` / `MEMORY` | `2` / `2g` | Raise these if your VMs are bigger |
| `spark.driver.host` | `nodeN` | Executors call back to the driver; it must advertise its ZeroTier name |

### 1.4 Start HDFS + Spark

Start node2 first, then node3 within a minute or so:

```bash
# node2 — formats the NameNode the very first time only
bash scripts/start_node.sh 2
# node3
bash scripts/start_node.sh 3
```

`jps` should then show:

* **node2:** `NameNode`, `DataNode`, `Worker`
* **node3:** `Master`, `Worker`, `DataNode`, `HistoryServer`

Verify the cluster from node2 or node3:

```bash
bash scripts/check_cluster.sh
```

Expect `Live datanodes (2)` and 2 ALIVE workers. You can also check the web UIs:

* HDFS: `http://node2:9870`
* Spark master: `http://node3:8080`
* History server: `http://node3:18080`

> To open these from your laptop, join your laptop to the same ZeroTier network (and authorize it) and use the ZeroTier IPs.

If node2's Worker came up before the Master and gave up, restart it on node2:

```bash
stop-worker.sh
start-worker.sh spark://node3:7077
```

### 1.5 Create the Iceberg tables (node2, once)

```bash
bash scripts/submit.sh jobs/init_tables.py
hdfs dfs -ls -R /warehouse | head        # metadata/ folders now exist in HDFS
```

This creates these tables in `lake.db`:

| Table | What it holds |
|---|---|
| `pages` | Latest version of each doc; MERGE target, bucketed by doc_id |
| `links` | Clean link graph (src, dst), both ends exist |
| `link_records` | Standalone link records, only used if your dataset has them |
| `rejects` | Bad input lines plus the reason, partitioned by phase |
| `ingest_log` | One audit row per ingestion run |
| `postings` | Inverted index: term → (doc_id, tf) |
| `doc_stats` | Document lengths for BM25 |
| `pagerank` | Rank per doc |
| `job_state` | Which snapshot each job last processed |

---

## Phase 2 — Node 1: Ingestion

### 2.0 Look at the data before ingesting it (2 minutes, saves hours)

```bash
mkdir -p ~/data   # put pages.jsonl and changes.jsonl here
python3 scripts/inspect_dataset.py ~/data/pages.jsonl ~/data/changes.jsonl
```

Compare the key names it prints with `FIELD_ALIASES` at the top of `jobs/ingest.py`:

* The defaults already accept `doc_id`/`id`, `url`, `title`, `text`/`content`, `version`, `links`/`outlinks`, and `op`/`action` (for deletes).
* If a key is named differently, add it to that list. Nothing else needs to change.
* If `links` holds **titles** instead of IDs, tell me (or map titles → ids before the explode in `build_link_graph`).

### 2.1 Phase 0: base corpus

```bash
bash scripts/node1_ingest_phase.sh 0 ~/data/pages.jsonl
```

The script:

1. Runs `hdfs dfs -put` to copy the file to `/input/`, forces replication 2, and prints `fsck` output (`repl=2`).
2. Submits `ingest.py` to `spark://node3:7077`. The driver runs here on node1 and the tasks run on node2 and node3.

Inside `ingest.py`:

| Step | How |
|---|---|
| Parse + validate | Each line goes through `json.loads` on the executors. Rejected with a reason: `malformed_json`, `not_a_json_object`, `empty_line`, `missing_doc_id`, `missing_or_invalid_version`, `missing_url/title/text`, `invalid_url`, `links_not_a_list`, `unknown_op`. Rejects go to `lake.db.rejects`. |
| Deduplicate | `row_number()` over `doc_id` ordered by `version DESC, line_no DESC`, so the highest version wins. |
| Diff vs Iceberg | Left-join on the current `pages` table. Each row gets `insert`, `update` (newer version), `delete` (op=delete and version ≥ current), or `noop`. |
| Merge | One `MERGE INTO lake.db.pages` = one atomic Iceberg snapshot. Only non-noop rows are merged. |
| Links | Self-links are dropped, then any link whose src or dst isn't in `pages` is dropped (two `left_semi` joins), then the result is written to `lake.db.links`. |
| Audit | Counts plus the new pages snapshot id go to `lake.db.ingest_log`. |

### 2.2 Phase 1: changes (only after Phase 0 has been served and observed)

```bash
bash scripts/node1_ingest_phase.sh 1 ~/data/changes.jsonl
```

Same job. Newer versions update, deletes remove, stale versions are ignored.

**Idempotency proof (show this in the demo):** run the exact same command again. The log says `actions={'noop': …}` and "No page changes -> skipping MERGE". No new snapshot is created and the table contents are unchanged.

---

## Phase 3 — Nodes 2 & 3: Indexing, PageRank and Export

Run everything on **node3**:

```bash
bash scripts/node3_process.sh          # index -> pagerank -> export; stops at the first failure
```

Or run the steps one at a time:

```bash
bash scripts/submit.sh jobs/index.py
bash scripts/submit.sh jobs/pagerank.py --iterations 20
bash scripts/submit.sh jobs/export.py
```

### 3.1 Inverted index (`index.py`)

* **Map:** tokenize each doc's title (counted twice) and text with `textutil.tokenize` (lowercase, `[a-z0-9]+`, stopwords removed).
* **Reduce:** `Counter` per doc, giving `(term, doc_id, tf)` rows, plus `(doc_id, doc_len)`.
* **Incremental update:**
  * `job_state` remembers which `pages` snapshot the index was built from.
  * On the next run, the job compares `pages VERSION AS OF <old>` with the current table (Iceberg time travel). That gives exactly the inserted, updated and deleted doc_ids.
  * Only those docs are re-tokenized. Postings for unchanged docs are kept.
  * If nothing changed, it exits ("already up to date").
  * Use `--full` to force a rebuild.
* Tested: the incremental result is byte-identical to a full rebuild.

### 3.2 PageRank (`pagerank.py`)

* Formula: PR(p) = (1−d)/N + d·(Σ PR(q)/out(q) + dangling_mass/N), with d = 0.85, 20 iterations by default (the job refuses fewer than 10).
* Dangling pages (no out-links) share their rank evenly, so ranks always sum to 1. The export checks this.
* Every 5 iterations the ranks are checkpointed to `hdfs:///checkpoints/pagerank`. This cuts the lineage, so a lost executor only recomputes from the last checkpoint.
* Each iteration logs the L1 change, so you can show convergence.
* Tested: matches a NumPy reference to within 1e-17.

### 3.3 Export (`export.py`): the gate between the lakehouse and serving

Before anything is written, it checks:

* The index and PageRank were computed from the **current** pages and links snapshots (via `job_state`).
* docs count == pages count, and no duplicate doc_ids.
* No null url, pagerank or doc_len.
* No posting points to an unknown doc.
* PageRank sums to 1 (±1e-3).

Then it:

1. Writes `/exports/.tmp_vN/{docs,postings}` as Parquet.
2. Reads them back and re-counts.
3. Writes `manifest.json`.
4. Atomically renames `.tmp_vN` → `/exports/vN` (an HDFS rename is atomic).

If anything fails, nothing is published and earlier versions are untouched.

---

## Phase 4 — Node 4: Search Service and UI

```bash
# 1. Start the API (first time). Edit User/WorkingDirectory in the unit file if you're not "ubuntu".
sudo cp search/search.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now search
#    (or, in a terminal:  cd search && SERVING_ROOT=/serving /opt/searchenv/bin/uvicorn app:app --host 0.0.0.0 --port 8000)

# 2. Pull the newest export and switch to it
bash search/pull_export.sh            # pulls v1 after Phase 0, v2 after Phase 1
bash search/pull_export.sh 1          # re-activate an older version (demo: before/after changes)
```

`pull_export.sh` does the following:

1. Runs `hdfs dfs -get` to copy the export into `/serving/.incoming_vN`.
2. Runs `verify_export.py` (manifest status plus row counts must match).
3. Renames it to `/serving/vN`.
4. Atomically flips the `/serving/current` symlink.
5. Sends `POST /admin/reload`.

If verification or the reload fails, the symlink is reverted and the old version keeps serving.

### The API

| Endpoint | Purpose |
|---|---|
| `GET /` | The search UI: box, ranked results with title, doc_id, URL and score breakdown |
| `GET /search?q=...&k=10` | JSON results |
| `GET /doc/{doc_id}` | One document's metadata |
| `GET /version` | Serving version, manifest and ranking config |
| `GET /health` | Liveness plus the last reload error |
| `POST /admin/reload` | Load `/serving/current`; all-or-nothing |

Open `http://node4:8000/` (or the node4 ZeroTier IP) in your browser.

### Ranking

The ranking code is in `search/app.py` → `Index.search`.

```
IDF(t)   = ln(1 + (N − df + 0.5)/(df + 0.5))
BM25     = Σ IDF(t) · tf·(k1+1) / (tf + k1·(1 − b + b·len/avgdl))      k1 = 1.2, b = 0.75
Final    = 0.7 · BM25/max(BM25) + 0.3 · PR/max(PR)
```

Only docs with BM25 > 0 are candidates.

* By default, `max(PR)` is taken over the matching docs, the same way BM25 is normalised. Set `PR_NORM=global` to normalise PageRank by the corpus-wide maximum instead.
* If your course's BM25 "code snippet" uses a different IDF variant, swap the one line in `Index.idf`.
* The query tokenizer `search/textutil.py` must stay identical to `jobs/textutil.py`. Run `bash scripts/sync_textutil.sh` after any change.

Tested: API scores match an independent brute-force BM25 + PageRank calculation, and a broken export is rejected while the old version keeps serving.

---

## The full demo script (what to run in front of the evaluator)

1. `bash scripts/check_cluster.sh` shows 2 DataNodes, 2 workers, and `repl=2`.
2. **Phase 0** (as above). Then search for a few queries and screenshot the results with `index v1`.
3. **Phase 1:** run `node1_ingest_phase.sh 1 …`, then `node3_process.sh`, then `pull_export.sh`. Repeat the same queries:
   * Updated docs move up or down.
   * Deleted docs are gone.
   * New docs appear.
   * The UI shows `index v2`.
4. **Re-ingest Phase 1** to show it's a no-op (idempotent).
5. **Versioning / time travel** (node2):

   ```bash
   bash scripts/submit.sh jobs/maintenance.py snapshots              # every snapshot of every table
   bash scripts/submit.sh jobs/maintenance.py diff --from <S0> --to <S1>   # what Phase 1 changed
   bash scripts/submit.sh jobs/maintenance.py rejects                # what was rejected and why
   ```

6. **Recover from a bad update:**
   1. Ingest a deliberately bad file as phase 2.
   2. Run `maintenance.py rollback --table pages --snapshot <S1>`.
   3. Rerun `node3_process.sh`, then `pull_export.sh`.
7. **Serving keeps the old version on failure:**
   * Run `pull_export.sh 999` (a version that doesn't exist). It fails, and `/health` still shows the old version.
   * Or stop a job mid-export: no `vN` appears.
8. **Spark fault tolerance:**
   1. Start `bash scripts/submit.sh jobs/pagerank.py --iterations 30` on node3.
   2. While it runs, on node2: `kill -9 $(jps | awk '/CoarseGrainedExecutorBackend/{print $1}')`.
   3. In `http://node3:4040`, the executor shows as lost and its tasks are re-run. The job still finishes with sum ≈ 1.0.
   4. For a bigger hit, use `stop-worker.sh` on node2. The job carries on with node3's worker alone.
9. **HDFS fault tolerance:**
   1. On node3, run `hdfs --daemon stop datanode`.
   2. From node1, `hdfs dfs -cat /input/pages.jsonl | head -2` still works because node2 holds the second replica.
   3. `hdfs fsck /` reports under-replicated blocks.
   4. Restart the DataNode with `hdfs --daemon start datanode`.
   5. Be honest in the viva: the NameNode on node2 is a single point of failure in this design.
10. **"Where the time and network traffic go":**
    * Open `http://node3:18080` (history server). For each job, show stage durations and *Shuffle Read/Write*.
    * The `[time]` lines in `logs/*.log` give per-step wall time.
    * Typically, the PageRank joins dominate shuffle and the MERGE dominates ingestion.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Live datanodes (0)` or DataNode log says "Retrying connect to node2:9000" | `/etc/hosts` still has `127.0.1.1 nodeX`. Re-run `setup_hosts.sh`, then `hdfs --daemon stop/start datanode`. Check `ss -tlnp \| grep 9000` on node2 shows `0.0.0.0:9000`. |
| DataNode won't start: "Incompatible clusterIDs" | You re-formatted the NameNode. On that DataNode: `rm -rf /data/hdfs/datanode/*`, then start it again. |
| Job hangs at "Initial job has not accepted any resources" | No worker registered, or the driver isn't reachable. Check `http://node3:8080`. Check `spark.driver.host` is `nodeN` in `/opt/spark/conf/spark-defaults.conf`. Run `sudo ufw status` (the ZT subnet must be allowed). |
| `ClassNotFoundException: org.apache.iceberg...` or `Catalog 'lake' not found` | Iceberg jar missing on that node: `ls /opt/spark/jars/iceberg*`. Every node 1–3 needs it. |
| `ModuleNotFoundError: textutil` / `common` | Use `scripts/submit.sh` (it passes `--py-files`). Run it from the project root. |
| Python version mismatch between driver and workers | All nodes must run the same Ubuntu, so the same `python3`. `PYSPARK_PYTHON=python3` is set. |
| `MERGE ... multiple source rows matched` | Can't happen after `dedup_latest`. If it does, you edited the dedup step. |
| Executors OOM | Lower `spark.executor.memory` if the VM has less than 4 GB, or raise `SPARK_WORKER_MEMORY` if it has more. Restart the workers. |
| `/search` returns 503 | Nothing has been pulled yet. Run `bash search/pull_export.sh`. |
| Results empty for an obvious word | The tokenizers differ. Run `bash scripts/sync_textutil.sh` and restart the API. |
| Export says "index is stale" | Working as designed: run `index.py` and `pagerank.py` first (or just run `node3_process.sh`). |

## File map

```
cluster.env               <- the ONLY file you must edit (IPs, user)
conf/                     Hadoop + Spark configs, hosts snippet
scripts/                  setup_hosts, install_node, start/stop/check, submit, per-node pipeline, inspect_dataset
jobs/                     common, textutil, init_tables, ingest, index, pagerank, export, maintenance
search/                   app.py (FastAPI + BM25), static/index.html, pull_export.sh, verify_export.py, search.service
```
