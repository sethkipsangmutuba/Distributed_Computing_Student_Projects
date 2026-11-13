# ICS 2403 — Distributed Computing & Applications
## Distributed 5G/6G Network Service Platform — Project Roadmap

**One system. Twelve evolutionary milestones.** Everything below builds on the
same `common/` codebase and the same 5-node topology (`edge-1, edge-2, ran-1,
core-1, cloud-1`) introduced in Milestone 1. Nothing is rebuilt from scratch.

## Status

| Deliverable | Milestones | Status |
|---|---|---|
| **Assignment 1** | M1 Distributed OS Foundation, M2 Processing & Performance | ✅ **Done** — `assignment1_m1_m2/` |
| **Assignment 2** | M3 Distributed Architecture, M4 Algorithms & Coordination | ✅ **Done** — `assignment2_m3_m4/` |
| **Assignment 3** | M5 Distributed Transactions, M6 Concurrency & Deadlock | ✅ **Done** — `assignment3_m5_m6/` |
| **Practical 1** | M7 Fault Tolerance & Recovery, M8 Distributed System Models | ⏳ Not started |
| **Practical 2** | M9 Transparency & Reliability, M10 Naming & Process Mgmt | ⏳ Not started |
| **Practical 3** | M11 RPC & Distributed Shared Memory, M12 Distributed File System / Capstone | ⏳ Not started |
| **CAT 1** | Technical report, M1–M12 (30-section structure from the brief) | ⏳ Not started (assemble once all milestones exist) |
| **CAT 2** | Conference-style research paper (derived from CAT 1, kept concise) | ⏳ Not started |
| **CAT 3** | Theoretical concept exam — curated question bank, M1–M12 | ⏳ Not started |

## Repository layout

```
5g6g_platform/
├── common/                        # shared engine — reused by every milestone
│   ├── node.py                    # Node = OS process + resource mgr + process mgr + comms
│   ├── resource_manager.py        # CPU/memory/bandwidth allocation ledger
│   ├── process_manager.py         # process creation, FIFO/priority scheduling
│   ├── communication.py           # TCP + length-prefixed JSON transport
│   ├── topology.py                # the ONE topology every milestone reuses
│   ├── workload_generator.py      # URLLC/eMBB/mMTC synthetic telecom workload
│   └── performance_monitor.py     # throughput/latency/jitter/loss/bottleneck metrics
├── assignment1_m1_m2/             # ✅ done
│   ├── milestone1_foundation.py
│   ├── milestone2_processing_performance.py
│   ├── ASSIGNMENT1_REPORT.md      # deliverables write-up + presentation summary
│   └── results/                   # real JSON output from running the scripts
├── assignment2_m3_m4/             # ✅ done (+ Big Data / Power BI extension)
│   ├── milestone3_architecture.py
│   ├── milestone4_coordination.py
│   ├── assignment2_bigdata_extension.py   # Spark batch layer + Power BI CSV exports (m3_/m4_ files)
│   ├── ASSIGNMENT2_REPORT.md      # deliverables write-up + presentation summary
│   └── results/                   # this assignment's own JSON logs only
├── assignment3_m5_m6/             # ✅ done (+ Big Data / Power BI extension)
│   ├── milestone5_transactions.py
│   ├── milestone6_concurrency_deadlock.py
│   ├── assignment3_bigdata_extension.py   # Spark batch layer + Power BI CSV exports (m5_/m6_ files)
│   ├── ASSIGNMENT3_REPORT.md      # deliverables write-up + presentation summary
│   └── results/                   # this assignment's own JSON logs only
├── practical1_m7_m8/               # next
├── practical2_m9_m10/
├── practical3_m11_m12/
├── cat1_technical_report/
├── cat2_research_paper/
├── cat3_theory/
├── results/                        # SHARED big-data outputs (project root, not per-assignment)
│   ├── bigdata/                    # raw Spark batch outputs, every assignment
│   └── powerbi/                    # flattened CSVs for Power BI, m3_/m4_/m5_/m6_/... prefixed
└── POWERBI_DASHBOARD_GUIDE.md      # the ONE master guide for the ONE growing dashboard
```

## Big Data & Power BI — the standing framework (every assignment, consistently)

Starting with Assignment 2 and continuing through every assignment after,
big data and Power BI are integrated the SAME way every time, through
three shared modules in `common/` (never duplicated or reinvented per
assignment):

| Module | Job |
|---|---|
| `common/spark_utils.py` | One shared way to start Spark (local mode) and run a distributed aggregation |
| `common/big_data_layer.py` | Each assignment's 100,000-row dataset generator(s) + Spark batch-layer function(s) |
| `common/powerbi_export.py` | Writes every assignment's CSVs into the ONE shared `results/powerbi/` folder |

Each assignment's `assignmentN_bigdata_extension.py` follows the identical
Lambda Architecture pattern: Batch layer (Spark, 100,000 rows) → Speed
layer (the real live platform) → Serving layer (CSVs, prefixed `mN_`) →
the SAME one Power BI dashboard (`POWERBI_DASHBOARD_GUIDE.md`, project
root), which simply gains new pages each time rather than being rebuilt.
Practical 1 onward will follow this exact same naming convention
(`practical1_bigdata_extension.py`, `m7_`/`m8_` prefixed files, and so on).

## How each future stage will extend the system (so evolution stays continuous)

* **M3 (Architecture):** ✅ done — service decomposition (`SERVICES_BY_TYPE`),
  deployment architecture, scalability + edge-vs-core trade-off experiments.
* **M4 (Coordination):** ✅ done — `common/coordination.py` (Lamport clocks +
  Bully leader election), wired into every node via `node.py`'s existing
  message handling. `election_id` ranking and `current_leader` state are now
  part of every node and will be reused by M5 (transaction coordinator) and
  M7 (automatic re-election on node failure).
* **M5 (Transactions):** ✅ done — `common/transaction.py` (Two-Phase Commit:
  prepare/commit/abort + session teardown), proven under normal operation
  and under a deliberate node failure.
* **M6 (Concurrency/Deadlock):** ✅ done — `common/lock_manager.py` (per-node
  locks) + `common/deadlock_detector.py` (distributed wait-for graph, cycle
  detection, victim-based recovery). Race condition vs mutual exclusion
  demonstrated directly.
* **M7 (Fault tolerance):** Milestone 5 already killed a node process
  on-demand as an experiment; M7 will formalize this into automatic
  failure DETECTION (rather than us manually calling `.terminate()`) plus
  MTTR/availability measurement, reusing the same node-as-OS-process design
  from M1 and the Bully re-election already built in M4.
* **M8 (Models):** re-run the M2 performance harness under host-based /
  processor-pool / server-based configurations of the same topology.
* **M9 (Transparency):** add a location/failure-transparent proxy in front of
  the existing node addresses.
* **M10 (Naming):** replace hardcoded `NODE_ADDR` lookups with a name-server
  node.
* **M11 (RPC/DSM):** formalize `communication.py`'s framing into a proper
  client-stub/server-stub RPC layer; add a shared distributed state store.
* **M12 (Capstone):** add a storage layer (simple distributed KV/file store)
  and run the Final Capstone Experiment (Section 5 of the brief) varying
  nodes/users/bandwidth/failure rate across the whole assembled system.

## Next step

Say "continue with Assignment 2" (or any specific milestone/practical/CAT) and
it will be built the same way: working Python code with `[M#]`-tagged console
output, results saved to disk, and a concise `ASSIGNMENTx_REPORT.md` with a
presentation-ready summary at the end.
