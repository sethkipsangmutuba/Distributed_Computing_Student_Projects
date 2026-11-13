"""
================================================================================
ASSIGNMENT 3 EXTENSION - Big Data Integration + Power BI Dashboard
================================================================================
System: Distributed 5G/6G Network Service Platform (Theme 1)
Extends: Milestone 5 (Distributed Transactions) and Milestone 6 (Concurrency/Deadlock)

Same framework, same pattern as assignment2_bigdata_extension.py -- this
is what "integrated consistently" means in practice: the SAME shared
modules (common/spark_utils.py, common/big_data_layer.py,
common/powerbi_export.py), the SAME Lambda Architecture, writing into
the SAME project-wide results/powerbi/ folder (filenames prefixed
m5_/m6_ so they sit alongside Assignment 2's m3_/m4_ files without
collision), feeding the SAME one growing Power BI dashboard.

    [BATCH LAYER]   Apache Spark processes two 100,000-row datasets:
                     simulated transaction attempts (M5) and simulated
                     lock requests (M6) -- common/big_data_layer.py
                          |
    [SPEED LAYER]    The SAME live platform from Milestones 1-6 runs
                     real 2PC transactions (M5) and a real distributed
                     deadlock scenario (M6) -- reusing the exact logic
                     from milestone5_transactions.py / milestone6_*.py
                          |
    [SERVING LAYER]  Batch + Speed + M5/M6 live results merged into
                     results/powerbi/m5_*.csv and m6_*.csv
                          |
    [PRESENTATION]   The SAME Power BI dashboard gains two new pages
                     (see the project-root POWERBI_DASHBOARD_GUIDE.md)

Run:
    python -m assignment3_m5_m6.assignment3_bigdata_extension
================================================================================
"""

import sys
import os
import json
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.node import start_node_process, wait_until_up
from common.topology import NodeConfig, SERVICES_BY_TYPE
from common.communication import send_message
from common.transaction import TwoPhaseCommitCoordinator
from common.deadlock_detector import DeadlockDetector
from common.big_data_layer import run_transaction_batch_layer, run_lock_contention_batch_layer
from common.powerbi_export import write_csv, write_metric_rows_csv, POWERBI_DIR

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")  # local JSON logs for THIS assignment only
os.makedirs(RESULTS_DIR, exist_ok=True)

N_ROWS = 100_000


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


def launch():
    for c in STANDARD_CONFIGS:
        start_node_process(c)
    for c in STANDARD_CONFIGS:
        wait_until_up(c.host, c.port, timeout=10)


# ---------------------------------------------------------------------------
# [SPEED LAYER - M5] a bigger live batch of real 2PC transactions
# ---------------------------------------------------------------------------

def speed_layer_transactions(n=50):
    log("SPEED", f"Replaying {n} real 2PC session-establishment transactions through the live platform ...")
    rows = []
    for i in range(n):
        edge = "edge-1" if i % 2 == 0 else "edge-2"
        participants = {
            edge: {"host": NODE_ADDR[edge][0], "port": NODE_ADDR[edge][1],
                   "reservation": {"cpu": 1, "memory": 64, "bandwidth": 5}},
            "ran-1": {"host": NODE_ADDR["ran-1"][0], "port": NODE_ADDR["ran-1"][1],
                       "reservation": {"cpu": 1, "memory": 128, "bandwidth": 10}},
        }
        coordinator = TwoPhaseCommitCoordinator(participants, send_message, prepare_timeout=1.5)
        outcome = coordinator.run()
        rows.append({"tx_id": outcome["tx_id"], "participant_edge": edge, "decision": outcome["decision"],
                      "elapsed_ms": outcome["elapsed_ms"]})
        if outcome["decision"] == "commit":
            for name, p in participants.items():
                try:
                    send_message(p["host"], p["port"], {"type": "tx_release", "tx_id": outcome["tx_id"]}, timeout=1.5)
                except Exception:
                    pass
    committed = len([r for r in rows if r["decision"] == "commit"])
    log("SPEED", f"  {committed}/{n} live transactions committed")
    return rows


# ---------------------------------------------------------------------------
# [SPEED LAYER - M6] the same real distributed deadlock scenario
# ---------------------------------------------------------------------------

def speed_layer_deadlock():
    log("SPEED", "Reproducing the live distributed deadlock scenario (Tx-A/Tx-B, edge-1/core-1) ...")
    resource = "session_slot"
    tx_a, tx_b = "Tx-A-bd", "Tx-B-bd"
    outcomes = {}

    def run_tx_a():
        r1 = send_message(*NODE_ADDR["edge-1"], {"type": "lock_acquire", "resource": resource,
                                                    "tx_id": tx_a, "timeout": 20.0}, timeout=25.0)
        outcomes["A_edge1"] = r1["status"]
        threading.Event().wait(1.0)
        r2 = send_message(*NODE_ADDR["core-1"], {"type": "lock_acquire", "resource": resource,
                                                    "tx_id": tx_a, "timeout": 20.0}, timeout=25.0)
        outcomes["A_core1"] = r2["status"]
        if r2["status"] == "granted":
            send_message(*NODE_ADDR["core-1"], {"type": "lock_release", "resource": resource, "tx_id": tx_a})
        send_message(*NODE_ADDR["edge-1"], {"type": "lock_release", "resource": resource, "tx_id": tx_a})

    def run_tx_b():
        r1 = send_message(*NODE_ADDR["core-1"], {"type": "lock_acquire", "resource": resource,
                                                    "tx_id": tx_b, "timeout": 20.0}, timeout=25.0)
        outcomes["B_core1"] = r1["status"]
        threading.Event().wait(1.0)
        r2 = send_message(*NODE_ADDR["edge-1"], {"type": "lock_acquire", "resource": resource,
                                                    "tx_id": tx_b, "timeout": 20.0}, timeout=25.0)
        outcomes["B_edge1"] = r2["status"]
        if r2["status"] == "granted":
            send_message(*NODE_ADDR["edge-1"], {"type": "lock_release", "resource": resource, "tx_id": tx_b})
        send_message(*NODE_ADDR["core-1"], {"type": "lock_release", "resource": resource, "tx_id": tx_b})

    t_a = threading.Thread(target=run_tx_a)
    t_b = threading.Thread(target=run_tx_b)
    t_a.start()
    t_b.start()
    import time
    time.sleep(1.5)
    detector = DeadlockDetector(NODE_ADDR, send_message)
    report = detector.run_detection_cycle()
    t_a.join(timeout=5)
    t_b.join(timeout=5)
    log("SPEED", f"  cycles found: {report['cycles_found']}, outcomes: {outcomes}")
    return {"outcomes": outcomes, "detection_report": report}


# ---------------------------------------------------------------------------
# [SERVING LAYER] export everything through the shared Power BI module
# ---------------------------------------------------------------------------

def export_m5_csvs(batch_summary, live_rows):
    write_csv("m5_batch_transactions_by_pair_scenario.csv", batch_summary["by_pair_scenario"])
    write_csv("m5_batch_transactions_by_scenario.csv", batch_summary["by_scenario"])
    write_csv("m5_live_transaction_log.csv", live_rows,
              fieldnames=["tx_id", "participant_edge", "decision", "elapsed_ms"])
    committed = len([r for r in live_rows if r["decision"] == "commit"])
    write_metric_rows_csv("m5_live_transaction_summary.csv", {
        "total_live_transactions": len(live_rows),
        "committed": committed,
        "commit_rate": round(committed / len(live_rows), 4) if live_rows else 0,
        "batch_rows_processed": batch_summary["total_rows_processed"],
        "spark_partitions_used": batch_summary["spark_partitions_used"],
    })
    log("SERVING", "Exported m5_batch_transactions_by_pair_scenario.csv, m5_batch_transactions_by_scenario.csv, "
                    "m5_live_transaction_log.csv, m5_live_transaction_summary.csv")


def export_m6_csvs(contention_summary, deadlock_result):
    write_csv("m6_batch_lock_contention_by_resource.csv", contention_summary["contention_by_resource"])

    graph_edges = [{"waiting_tx": k, "waiting_for_tx": v}
                   for k, vs in deadlock_result["detection_report"]["wait_for_graph"].items() for v in vs]
    write_csv("m6_live_wait_for_graph.csv", graph_edges, fieldnames=["waiting_tx", "waiting_for_tx"])

    cycles = deadlock_result["detection_report"]["cycles_found"]
    write_metric_rows_csv("m6_live_deadlock_summary.csv", {
        "cycles_found": len(cycles),
        "victim_chosen": deadlock_result["detection_report"]["resolutions"][0]["victim"] if cycles else "none",
        "detection_elapsed_ms": deadlock_result["detection_report"]["detection_elapsed_ms"],
        "batch_rows_processed": contention_summary["total_rows_processed"],
        "spark_partitions_used": contention_summary["spark_partitions_used"],
    })
    log("SERVING", "Exported m6_batch_lock_contention_by_resource.csv, m6_live_wait_for_graph.csv, "
                    "m6_live_deadlock_summary.csv")


def main():
    log("BATCH", f"[BIG DATA] Running Apache Spark batch layer over {N_ROWS:,} simulated transaction attempts ...")
    m5_batch = run_transaction_batch_layer(n_rows=N_ROWS)
    log("BATCH", f"  Spark processed {m5_batch['total_rows_processed']:,} rows across "
                  f"{m5_batch['spark_partitions_used']} partitions")
    for row in m5_batch["by_scenario"]:
        log("BATCH", f"    scenario={row['scenario']:10s} attempts={row['attempt_count']:6d}  "
                      f"predicted_commit_rate={row['predicted_commit_rate']}")

    log("BATCH", f"[BIG DATA] Running Apache Spark batch layer over {N_ROWS:,} simulated lock requests ...")
    m6_batch = run_lock_contention_batch_layer(n_rows=N_ROWS)
    top = sorted(m6_batch["contention_by_resource"], key=lambda r: -r["request_count"])[:3]
    for row in top:
        log("BATCH", f"    node={row['node']:8s} resource={row['resource']:14s} "
                      f"requests={row['request_count']:6d} distinct_tx={row['distinct_transactions']}")

    log("SPEED", "Launching the live 5-node platform (same as Milestones 1-6) ...")
    launch()
    m5_live = speed_layer_transactions(n=50)
    m6_live = speed_layer_deadlock()

    log("SERVING", "[BIG DATA] Merging batch + live M5/M6 results into Power BI-ready CSVs ...")
    export_m5_csvs(m5_batch, m5_live)
    export_m6_csvs(m6_batch, m6_live)

    with open(os.path.join(RESULTS_DIR, "bigdata_extension_summary.json"), "w") as f:
        json.dump({"n_rows": N_ROWS, "m5_batch": m5_batch, "m5_live": m5_live,
                    "m6_batch": m6_batch, "m6_live": m6_live}, f, indent=2, default=str)

    log("SERVING", f"All Power BI CSVs written to: {POWERBI_DIR}")
    log("SERVING", "ASSIGNMENT 3 BIG DATA + POWER BI EXTENSION COMPLETE.")


if __name__ == "__main__":
    main()
