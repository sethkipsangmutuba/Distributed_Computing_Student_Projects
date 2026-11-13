# Power BI Dashboard — Master Build Guide

This is the **one** Power BI dashboard for the whole project. Every assignment's big-data extension script writes its CSVs
into the same shared folder, `results/powerbi/`, at the project root
(never inside an individual `assignmentN_.../` folder), with filenames
prefixed by which milestone produced them (`m3_`, `m4_`, `m5_`, `m6_`, and
so on for every assignment still to come). That prefix convention is what
lets files from every assignment sit in one folder without colliding.

> Power BI Desktop files (`.pbix`) are a binary format created by the
> Power BI Desktop application itself — no script can generate one. This
> guide is used for building it by hand, once, and then just
> refreshing/extending it as each assignment adds new files.

## How this stays consistent, assignment to assignment

Every assignment's big-data extension script (`assignment2_bigdata_extension.py`,
`assignment3_bigdata_extension.py`, and so on) is built the same way, through
the same three shared modules:

| Module | Job |
|---|---|
| `common/spark_utils.py` | Starts Spark the same way every time; runs a distributed groupBy/aggregate |
| `common/big_data_layer.py` | Generates each assignment's 100,000-row dataset(s) and runs its Spark batch layer |
| `common/powerbi_export.py` | Writes every assignment's CSVs into the ONE shared `results/powerbi/` folder |

This means: **you only connect Power BI to `results/powerbi/` once.** Every
later assignment's extension script just adds more files to that same
folder — on your next "Refresh" in Power BI, or when you add the new file
as a new table, the dashboard grows without anything earlier breaking.

## Prerequisites

* Power BI Desktop installed
* You have run each assignment's big-data extension script at least once:
  - `python -m assignment2_m3_m4.assignment2_bigdata_extension`
  - `python -m assignment3_m5_m6.assignment3_bigdata_extension`
  - (repeat for each future assignment's equivalent script)

## One-time setup: connect Power BI to the shared folder

1. Power BI Desktop → **Get Data** → **Folder**.
2. Browse to `results/powerbi/` at the project root (NOT inside any
   `assignmentN_.../` subfolder) and select it.
3. Power BI shows a list of every CSV in that folder. Click **Combine &
   Transform Data** if you want them auto-merged by shape, but for this
   project it's clearer to instead click **Transform Data**, then in the
   Power Query Editor load each CSV as its own separate table (right-click
   → duplicate the query per file, point each at one row, is the simplest
   path for beginners — or just repeat **Get Data → Text/CSV** once per
   file instead of using the Folder connector, if that feels more direct).

Either approach is fine — what matters is that every file lives under the
one `results/powerbi/` path, so nothing needs reconnecting later.

## Assignment 2 pages (Milestones 3 & 4)

**Scalability (M3)** — uses `m3_scalability_results.csv`:
- Clustered Column Chart: Axis = `n_edge_nodes`, Values = `packet_loss_rate`
- Line Chart: Axis = `n_edge_nodes`, Values = `throughput_tasks_per_sec` and `mean_latency_ms`

**Coordination (M4)** — uses `m4_election_results.csv`:
- Table: all columns (scenario, initiator, elected_leader, rounds, total_messages, elapsed_ms, consistent_across_all_nodes)
- Clustered Bar Chart: Axis = `initiator`, Values = `total_messages`

**Architecture (M3)** — uses `m3_architecture_services.csv`:
- Matrix: Rows = `node_type` then `node`, Values = `service`

**Batch Layer — 100K rows (M3, Spark)** — uses `batch_summary_by_service_class.csv`
and `batch_summary_by_node.csv` (in `results/bigdata/`, imported the same way):
- Donut Chart: Legend = `service_class`, Values = `task_count`
- Clustered Column Chart: Axis = `assigned_node`, Values = `total_cpu_demand` and `total_bandwidth_demand_mbps`

**Speed Layer (Live Sample)** — uses `m3_speed_layer_summary.csv` and `m3_speed_layer_task_log.csv`:
- Cards for throughput/latency/jitter/loss (pivot the metric/value table first: Transform Data → select `metric` column → Transform → Pivot Column, values from `value`)
- Stacked Bar Chart: Axis = `node`, Legend = `status`, Values = Count of `status`

## Assignment 3 pages (Milestones 5 & 6)

**Transactions — Batch (M5, Spark, 100K rows)** — uses
`m5_batch_transactions_by_scenario.csv` and `m5_batch_transactions_by_pair_scenario.csv`:
- Clustered Column Chart: Axis = `scenario`, Values = `predicted_commit_rate` — shows the
  `node_down` scenario's commit rate collapsing to 0 at 100,000-row scale,
  the same finding the live experiment showed at small scale.
- Table: `pair`, `scenario`, `attempt_count`, `total_bandwidth_requested_mbps`

**Transactions — Live (M5)** — uses `m5_live_transaction_log.csv` and `m5_live_transaction_summary.csv`:
- Card: `commit_rate`
- Stacked Bar Chart: Axis = `participant_edge`, Legend = `decision`, Values = Count

**Deadlock — Batch Contention (M6, Spark, 100K rows)** — uses
`m6_batch_lock_contention_by_resource.csv`:
- Clustered Bar Chart: Axis = `resource` (or `node` + `resource` combined),
  Values = `request_count` — ranks which resources see the heaviest
  simulated contention across 100,000 lock requests.

**Deadlock — Live Detection (M6)** — uses `m6_live_wait_for_graph.csv` and `m6_live_deadlock_summary.csv`:
- Table: `waiting_tx`, `waiting_for_tx` — the actual wait-for graph edges
  from the live detected deadlock (should show the 2-edge cycle:
  Tx-A-bd → Tx-B-bd and Tx-B-bd → Tx-A-bd).
- Card: `cycles_found`, `victim_chosen`, `detection_elapsed_ms`

## Pattern for every future assignment

When a new assignment's extension script runs (e.g. a hypothetical
`assignment4_bigdata_extension.py`), it will write new `m7_*.csv` /
`m8_*.csv` files into the exact same `results/powerbi/` folder. To add its
pages: **Home → Refresh** picks up any files that existed when you first
connected; for genuinely NEW files, use **Get Data → Text/CSV** once per
new file (or re-point the Folder connector at the same path, which will
pick up new files automatically) and build new chart pages the same way
as above. The dashboard never needs to be rebuilt from scratch.

## Save and share

Save as `5g6g_platform_dashboard.pbix` at the project root (not inside any
assignment subfolder, since it now covers multiple assignments). Commit
and push it to GitHub alongside the code and reports, same as any other
file — see your existing Source Control workflow.
