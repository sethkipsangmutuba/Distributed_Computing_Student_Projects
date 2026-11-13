"""
================================================================================
ASSIGNMENT 3 | MILESTONE 6 - Concurrency and Deadlock Management (Week 6)
================================================================================
System: Distributed 5G/6G Network Service Platform (Theme 1)

WHAT THIS SCRIPT DEMONSTRATES (per the milestone brief):
  [M6-a] Race condition: many threads incrementing an unprotected shared
         counter lose updates; the SAME operation protected by a lock
         (mutual exclusion) does not.
  [M6-b] Resource contention + deadlock: two transactions each need
         locks on two DIFFERENT nodes, acquired in OPPOSITE order --
         the classic circular-wait setup.
  [M6-c] Deadlock DETECTION: a distributed detector polls every node's
         lock state, builds one system-wide wait-for graph (P_i -> P_j
         edges), and finds the cycle.
  [M6-d] Deadlock RECOVERY: the detector picks a victim transaction and
         force-aborts it, releasing its lock so the other transaction
         can proceed.
  [M6-e] Resource-allocation graph + resource utilization analysis.

Run:
    python -m assignment3_m5_m6.milestone6_concurrency_deadlock
================================================================================
"""

import sys
import os
import json
import time
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.node import start_node_process, wait_until_up
from common.topology import NodeConfig, SERVICES_BY_TYPE
from common.communication import send_message
from common.deadlock_detector import DeadlockDetector

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)


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
# [M6-a] Race condition vs mutual exclusion
# ---------------------------------------------------------------------------

def race_condition_experiment():
    log("M6", "Race condition experiment: 8 threads x 20 increments each (expected total: 160) ...")

    unsafe_counter = {"value": 0}

    def unsafe_increment():
        for _ in range(20):
            current = unsafe_counter["value"]
            time.sleep(0.001)  # forces a thread interleaving -> exposes the lost-update race
            unsafe_counter["value"] = current + 1

    threads = [threading.Thread(target=unsafe_increment) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    unsafe_result = unsafe_counter["value"]
    log("M6", f"  WITHOUT a lock (no mutual exclusion): final value = {unsafe_result} "
               f"(expected 160 -> {160 - unsafe_result} updates LOST to the race)")

    safe_counter = {"value": 0}
    lock = threading.Lock()

    def safe_increment():
        for _ in range(20):
            with lock:
                current = safe_counter["value"]
                time.sleep(0.001)
                safe_counter["value"] = current + 1

    threads = [threading.Thread(target=safe_increment) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    safe_result = safe_counter["value"]
    log("M6", f"  WITH a lock (mutual exclusion): final value = {safe_result} (expected 160 -> correct)")

    return {"expected": 160, "unsafe_result": unsafe_result, "updates_lost": 160 - unsafe_result,
            "safe_result": safe_result, "mutual_exclusion_correct": safe_result == 160}


# ---------------------------------------------------------------------------
# [M6-b/c/d] Distributed deadlock: create it, detect it, resolve it
# ---------------------------------------------------------------------------

def deadlock_experiment():
    log("M6", "Distributed deadlock experiment: Tx-A and Tx-B lock the SAME two resources "
               "on edge-1 and core-1, in OPPOSITE order ...")

    resource_on_edge = "session_slot"
    resource_on_core = "session_slot"
    tx_a, tx_b = "Tx-A", "Tx-B"

    results = {}

    HOLD_BEFORE_SECOND_ACQUIRE = 1.0  # forces real overlap: both threads must grab their FIRST lock
                                        # and still be holding it when they each reach for their SECOND

    def run_tx_a():
        # Tx-A: lock edge-1 first, HOLD, then reach for core-1
        r1 = send_message(*NODE_ADDR["edge-1"], {"type": "lock_acquire", "resource": resource_on_edge,
                                                    "tx_id": tx_a, "timeout": 20.0}, timeout=25.0)
        results["A_edge1"] = r1["status"]
        time.sleep(HOLD_BEFORE_SECOND_ACQUIRE)  # <-- guarantees Tx-B has time to grab core-1 first
        r2 = send_message(*NODE_ADDR["core-1"], {"type": "lock_acquire", "resource": resource_on_core,
                                                    "tx_id": tx_a, "timeout": 20.0}, timeout=25.0)
        results["A_core1"] = r2["status"]
        if r2["status"] == "granted":
            send_message(*NODE_ADDR["core-1"], {"type": "lock_release", "resource": resource_on_core, "tx_id": tx_a})
        send_message(*NODE_ADDR["edge-1"], {"type": "lock_release", "resource": resource_on_edge, "tx_id": tx_a})

    def run_tx_b():
        # Tx-B: lock core-1 first, HOLD, then reach for edge-1 -- OPPOSITE order from Tx-A -> circular wait
        r1 = send_message(*NODE_ADDR["core-1"], {"type": "lock_acquire", "resource": resource_on_core,
                                                    "tx_id": tx_b, "timeout": 20.0}, timeout=25.0)
        results["B_core1"] = r1["status"]
        time.sleep(HOLD_BEFORE_SECOND_ACQUIRE)  # <-- guarantees Tx-A has time to grab edge-1 first
        r2 = send_message(*NODE_ADDR["edge-1"], {"type": "lock_acquire", "resource": resource_on_edge,
                                                    "tx_id": tx_b, "timeout": 20.0}, timeout=25.0)
        results["B_edge1"] = r2["status"]
        if r2["status"] == "granted":
            send_message(*NODE_ADDR["edge-1"], {"type": "lock_release", "resource": resource_on_edge, "tx_id": tx_b})
        send_message(*NODE_ADDR["core-1"], {"type": "lock_release", "resource": resource_on_core, "tx_id": tx_b})

    t_a = threading.Thread(target=run_tx_a)
    t_b = threading.Thread(target=run_tx_b)
    t_a.start()
    t_b.start()  # started together (no stagger needed now -- the HOLD above guarantees the overlap)

    # Let the deadlock actually form: both threads grab their first lock almost
    # immediately, hold it for HOLD_BEFORE_SECOND_ACQUIRE seconds, then BOTH
    # reach for the other's lock at roughly the same time -> genuine circular
    # wait. We check shortly after that point, once both should be blocked.
    time.sleep(HOLD_BEFORE_SECOND_ACQUIRE + 0.5)
    still_blocked = t_a.is_alive() and t_b.is_alive()
    log("M6", f"  {HOLD_BEFORE_SECOND_ACQUIRE + 0.5:.1f}s in: both transactions still blocked (deadlocked) = {still_blocked}")

    # [M6-c] DETECT: build the system-wide wait-for graph and look for a cycle.
    detector = DeadlockDetector(NODE_ADDR, send_message)
    detection_report = detector.run_detection_cycle()
    log("M6", f"  wait-for graph: {detection_report['wait_for_graph']}")
    log("M6", f"  cycles found: {detection_report['cycles_found']}")

    # [M6-d] RECOVER: detector already broadcast mark_victim for each cycle found.
    if detection_report["resolutions"]:
        log("M6", f"  resolution: victim chosen = {detection_report['resolutions'][0]['victim']} "
                   f"(force-aborted to break the cycle)")

    t_a.join(timeout=5)
    t_b.join(timeout=5)
    both_finished = not t_a.is_alive() and not t_b.is_alive()
    log("M6", f"  both transactions finished after resolution: {both_finished}")
    log("M6", f"  final outcomes: {results}")

    return {
        "still_blocked_after_1s": still_blocked,
        "detection_report": detection_report,
        "final_outcomes": results,
        "both_transactions_finished_after_resolution": both_finished,
    }


# ---------------------------------------------------------------------------
# [M6-e] Resource-allocation graph + utilization, under normal concurrency
# ---------------------------------------------------------------------------

def resource_allocation_graph_demo():
    """
    A smaller, NON-deadlocking scenario purely to render a clean
    P_i -> R_j resource-allocation graph for the report: 3 concurrent
    "processes" each request one distinct resource on edge-1 -- no
    contention, all granted immediately, graph is a simple star (no
    cycles), included as the baseline picture to contrast against the
    deadlock experiment's cyclic graph above.
    """
    log("M6", "Resource-allocation graph demo: 3 processes, 3 distinct resources on edge-1, no contention ...")
    edges = []
    for i, res in enumerate(["cpu_pool", "memory_pool", "bandwidth_pool"]):
        tx_id = f"P{i+1}"
        reply = send_message(*NODE_ADDR["edge-1"], {"type": "lock_acquire", "resource": res,
                                                       "tx_id": tx_id, "timeout": 2.0})
        edges.append({"process": tx_id, "resource": res, "node": "edge-1", "status": reply["status"]})
    for e in edges:
        send_message(*NODE_ADDR["edge-1"], {"type": "lock_release", "resource": e["resource"], "tx_id": e["process"]})
    log("M6", f"  allocation graph edges (P_i -> R_j, all granted): {[(e['process'], e['resource']) for e in edges]}")
    return edges


def main():
    log("M6", "Launching the standard 5-node platform ...")
    launch()

    race_results = race_condition_experiment()
    graph_demo = resource_allocation_graph_demo()
    deadlock_results = deadlock_experiment()

    # Resource utilization snapshot at the end, for the "resource utilization analysis" deliverable
    utilization = {}
    for c in STANDARD_CONFIGS:
        mx = send_message(c.host, c.port, {"type": "metrics_request"})
        utilization[c.name] = mx.get("resource_utilization", {})

    out_path = os.path.join(RESULTS_DIR, "milestone6_concurrency_deadlock.json")
    with open(out_path, "w") as f:
        json.dump({
            "race_condition_experiment": race_results,
            "resource_allocation_graph_baseline": graph_demo,
            "deadlock_experiment": deadlock_results,
            "final_resource_utilization": utilization,
        }, f, indent=2, default=str)
    log("M6", f"Results saved -> {out_path}")
    log("M6", "MILESTONE 6 COMPLETE.")


if __name__ == "__main__":
    main()
