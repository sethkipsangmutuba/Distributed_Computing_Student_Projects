"""
================================================================================
ASSIGNMENT 2 EXTENSION - Big Data Integration + Power BI Dashboard
================================================================================
System: Distributed 5G/6G Network Service Platform (Theme 1)
Extends: Milestone 3 (Distributed Architecture) and Milestone 4 (Coordination)

This script does NOT replace milestone3_architecture.py or
milestone4_coordination.py -- both still run standalone and satisfy the
brief's M3/M4 objectives exactly as before. This script ADDS a big-data
batch layer and a Power BI-ready export layer on top, following a
Lambda Architecture -- the SAME pattern every assignment's big-data
extension follows from here on (see assignment3_m5_m6's equivalent
script), through the SAME shared modules (common/spark_utils.py,
common/big_data_layer.py, common/powerbi_export.py):

    [BATCH LAYER]   Apache Spark processes a 100,000-row historical
                     telecom workload dataset (common/big_data_layer.py)
                          |
    [SPEED LAYER]    The SAME live Edge/RAN/Core/Cloud node platform
                     from Milestones 1-4 processes a real-time,
                     stratified sample of that dataset
                          |
    [SERVING LAYER]  Batch + Speed + M3 scalability + M4 coordination
                     results are all merged into clean CSVs, written
                     through common/powerbi_export.py into the ONE
                     project-wide results/powerbi/ folder every
                     assignment's extension script shares (filenames
                     prefixed m3_/m4_ so nothing collides with later
                     assignments' m5_/m6_ files, etc.)
                          |
    [PRESENTATION]   Power BI Desktop imports that ONE growing folder
                     -> ONE dashboard that gains new pages as the
                     project progresses (see the project-root
                     POWERBI_DASHBOARD_GUIDE.md for the exact steps)

Run:
    python -m assignment2_m3_m4.assignment2_bigdata_extension
================================================================================
"""

import sys
import os
import json
import concurrent.futures
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.node import start_node_process, wait_until_up
from common.topology import NodeConfig, SERVICES_BY_TYPE
from common.communication import send_message
from common.big_data_layer import generate_large_dataset, run_batch_layer
from common.performance_monitor import RunRecorder
from common.powerbi_export import write_csv, write_metric_rows_csv, flatten_json_result, POWERBI_DIR, BIGDATA_DIR

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")  # local JSON logs for THIS assignment only

N_ROWS = 100_000
SAMPLE_SIZE = 600  # tasks actually replayed live through the socket platform


def log(tag, msg):
    print(f"[{tag}] {msg}")


STANDARD_CONFIGS = [
    NodeConfig("edge-1", "EDGE", "127.0.0.1", 9101, 8, 3072, 150, "priority", 0.0, 10, SERVICES_BY_TYPE["EDGE"]),
    NodeConfig("edge-2", "EDGE", "127.0.0.1", 9102, 8, 3072, 150, "priority", 0.0, 11, SERVICES_BY_TYPE["EDGE"]),
    NodeConfig("ran-1", "RAN", "127.0.0.1", 9201, 12, 6144, 500, "priority", 0.0, 20, SERVICES_BY_TYPE["RAN"]),
    NodeConfig("core-1", "CORE", "127.0.0.1", 9301, 16, 8192, 1000, "priority", 0.0, 30, SERVICES_BY_TYPE["CORE"]),
    NodeConfig("cloud-1", "CLOUD", "127.0.0.1", 9401, 32, 16384, 5000, "priority", 0.0, 40, SERVICES_BY_TYPE["CLOUD"]),
]
NODE_ADDR = {c.name: (c.host, c.port) for c in STANDARD_CONFIGS}


# ---------------------------------------------------------------------------
# [SPEED LAYER] stratified sample of the 100,000-row dataset, replayed LIVE
# ---------------------------------------------------------------------------

def stratified_sample(pdf, n, seed=99):
    """
    Proportional sample by service_class, so the live sample keeps the
    same traffic mix as the full 100,000-row dataset. Implemented as an
    explicit per-class loop (rather than groupby().apply()) since pandas
    3.x's apply() no longer reliably passes the grouping column through.
    """
    frac = n / len(pdf)
    parts = []
    for cls in pdf["service_class"].unique():
        subset = pdf[pdf["service_class"] == cls]
        parts.append(subset.sample(frac=frac, random_state=seed))
    return pd.concat(parts, ignore_index=True)


def run_speed_layer(pdf):
    log("SPEED", f"Launching the live 5-node platform (same as Milestones 1-4) ...")
    for c in STANDARD_CONFIGS:
        start_node_process(c)
    for c in STANDARD_CONFIGS:
        wait_until_up(c.host, c.port, timeout=10)

    sample = stratified_sample(pdf, SAMPLE_SIZE)
    log("SPEED", f"Replaying a stratified live sample of {len(sample)} tasks "
                  f"(same service-class mix as the full 100,000-row dataset) through the real node platform ...")

    edge_i = 0
    placement = []
    for _, row in sample.iterrows():
        task = {"name": f"task-{int(row['task_id'])}", "service_class": row["service_class"],
                "cpu": row["cpu"], "memory": row["memory"], "bandwidth": row["bandwidth"],
                "work_units": min(row["work_units"], 40)}  # capped so 600 live tasks finish quickly
        if task["service_class"] == "eMBB":
            node = "ran-1"
        else:
            node = "edge-1" if edge_i % 2 == 0 else "edge-2"
            edge_i += 1
        placement.append((task, node))

    recorder = RunRecorder()
    recorder.start()

    def submit(task, node_name):
        host, port = NODE_ADDR[node_name]
        try:
            reply = send_message(host, port, {"type": "task_request", "task": task}, timeout=5.0)
            status = reply.get("status", "error")
            latency = reply.get("server_latency_ms", reply.get("_rtt_ms", 0.0))
            return node_name, task["service_class"], status, latency
        except Exception:
            return node_name, task["service_class"], "error", 0.0

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(submit, t, n) for t, n in placement]
        for fut in concurrent.futures.as_completed(futures):
            node_name, svc, status, latency = fut.result()
            recorder.record(node_name, svc, status, latency)
    recorder.stop()

    node_utilization = {}
    for c in STANDARD_CONFIGS:
        node_utilization[c.name] = send_message(c.host, c.port, {"type": "metrics_request"})

    summary = recorder.summary(node_utilization)
    log("SPEED", f"Live sample complete: throughput={summary['throughput_tasks_per_sec']} tasks/s, "
                  f"mean_latency={summary['latency']['mean_ms']} ms, loss={summary['packet_loss_rate']*100:.1f}%")
    return summary, recorder.records


# ---------------------------------------------------------------------------
# [SERVING LAYER] export everything to Power BI-ready CSVs
# ---------------------------------------------------------------------------

def export_speed_layer_csvs(speed_summary, raw_records):
    path1 = write_csv("m3_speed_layer_task_log.csv", raw_records,
                       fieldnames=["node", "service_class", "status", "latency_ms"])
    path2 = write_metric_rows_csv("m3_speed_layer_summary.csv", {
        "wall_clock_seconds": speed_summary["wall_clock_seconds"],
        "throughput_tasks_per_sec": speed_summary["throughput_tasks_per_sec"],
        "mean_latency_ms": speed_summary["latency"]["mean_ms"],
        "p95_latency_ms": speed_summary["latency"]["p95_ms"],
        "jitter_ms": speed_summary["latency"]["jitter_ms"],
        "packet_loss_rate": speed_summary["packet_loss_rate"],
    })
    log("SERVING", f"Exported {os.path.basename(path1)} ({len(raw_records)} rows) and {os.path.basename(path2)}.")


def export_scalability_csv():
    """Re-flattens milestone3_architecture.py's saved JSON into a Power BI-friendly CSV, if present."""
    src = os.path.join(RESULTS_DIR, "milestone3_architecture.json")
    if not os.path.exists(src):
        log("SERVING", "milestone3_architecture.json not found yet -- run milestone3_architecture.py first "
                        "to populate m3_scalability_results.csv for the dashboard.")
        return

    def build_rows(data):
        rows = []
        for key, vals in data.get("scalability_experiment", {}).items():
            n_edges = key.split("_")[0]
            rows.append({"n_edge_nodes": n_edges, **vals})
        return rows

    rows = flatten_json_result(src, build_rows)
    path = write_csv("m3_scalability_results.csv", rows,
                      fieldnames=["n_edge_nodes", "throughput_tasks_per_sec",
                                   "mean_latency_ms", "p95_latency_ms", "packet_loss_rate"])
    log("SERVING", f"Exported {os.path.basename(path)} ({len(rows)} rows) from Milestone 3 results.")


def export_election_csv():
    """Re-flattens milestone4_coordination.py's saved JSON into a Power BI-friendly CSV, if present."""
    src = os.path.join(RESULTS_DIR, "milestone4_coordination.json")
    if not os.path.exists(src):
        log("SERVING", "milestone4_coordination.json not found yet -- run milestone4_coordination.py first "
                        "to populate m4_election_results.csv for the dashboard.")
        return

    def build_rows(data):
        rows = []
        for key, run in data.get("leader_election", {}).items():
            rows.append({
                "scenario": key, "initiator": run["initiator"], "elected_leader": run["declared_leader"],
                "rounds": run["rounds"], "total_messages": run["total_messages"],
                "elapsed_ms": run["elapsed_ms"], "consistent_across_all_nodes": run["consistency"]["consistent"],
            })
        return rows

    rows = flatten_json_result(src, build_rows)
    path = write_csv("m4_election_results.csv", rows,
                      fieldnames=["scenario", "initiator", "elected_leader", "rounds",
                                   "total_messages", "elapsed_ms", "consistent_across_all_nodes"])
    log("SERVING", f"Exported {os.path.basename(path)} ({len(rows)} rows) from Milestone 4 results.")


def export_architecture_csv():
    rows = []
    for c in STANDARD_CONFIGS:
        for svc in c.services:
            rows.append({"node": c.name, "node_type": c.node_type, "service": svc, "election_id": c.election_id})
    path = write_csv("m3_architecture_services.csv", rows,
                      fieldnames=["node", "node_type", "service", "election_id"])
    log("SERVING", f"Exported {os.path.basename(path)} ({len(rows)} rows).")


def main():
    log("BATCH", f"[BIG DATA] Generating {N_ROWS:,}-row telecom workload dataset (vectorized) ...")
    pdf = generate_large_dataset(n_rows=N_ROWS)
    log("BATCH", f"Dataset generated: {len(pdf):,} rows, {pdf.memory_usage(deep=True).sum() / 1e6:.1f} MB in memory")

    log("BATCH", "[BIG DATA] Running Apache Spark (local[*]) batch layer over the full dataset ...")
    batch_summary = run_batch_layer(n_rows=N_ROWS, results_dir=BIGDATA_DIR)
    log("BATCH", f"  Spark processed {batch_summary['total_rows_processed']:,} rows across "
                  f"{batch_summary['spark_partitions_used']} partitions")
    for row in batch_summary["by_service_class"]:
        log("BATCH", f"    {row['service_class']:6s} count={row['task_count']:6d}  "
                      f"avg_work_units={row['avg_work_units']}  total_bw_demand={row['total_bandwidth_demand_mbps']} Mbps")
    for row in batch_summary["by_node"]:
        log("BATCH", f"    {row['assigned_node']:8s} simulated_load={row['task_count']:6d} tasks  "
                      f"total_cpu_demand={row['total_cpu_demand']}")

    speed_summary, raw_records = run_speed_layer(pdf)

    log("SERVING", "[BIG DATA] Merging batch + speed + M3/M4 results into Power BI-ready CSVs ...")
    export_speed_layer_csvs(speed_summary, raw_records)
    export_scalability_csv()
    export_election_csv()
    export_architecture_csv()

    with open(os.path.join(BIGDATA_DIR, "batch_layer_summary.json"), "w") as f:
        json.dump(batch_summary, f, indent=2, default=str)
    with open(os.path.join(RESULTS_DIR, "bigdata_extension_summary.json"), "w") as f:
        json.dump({"n_rows": N_ROWS, "sample_size": SAMPLE_SIZE,
                    "batch_layer": batch_summary, "speed_layer": speed_summary}, f, indent=2, default=str)

    log("SERVING", f"All Power BI CSVs written to: {POWERBI_DIR}")
    log("SERVING", "BIG DATA + POWER BI EXTENSION COMPLETE.")


if __name__ == "__main__":
    main()
