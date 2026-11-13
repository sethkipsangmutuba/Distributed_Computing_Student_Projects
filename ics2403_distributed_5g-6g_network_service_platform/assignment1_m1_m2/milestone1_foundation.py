"""
================================================================================
ASSIGNMENT 1 | MILESTONE 1 - Distributed Operating System Foundation (Week 1)
================================================================================
System: Distributed 5G/6G Network Service Platform (Theme 1)

WHAT THIS SCRIPT DEMONSTRATES (per the milestone brief):
  [M1-a] Multiple computational nodes running as independent OS processes
  [M1-b] Process creation (Process objects with pid, state, resource footprint)
  [M1-c] Process scheduling (priority scheduling: URLLC > eMBB > mMTC)
  [M1-d] Resource allocation (CPU / memory / bandwidth ledger per node)
  [M1-e] Basic inter-node communication (TCP + JSON request/reply)

Run:
    python3 -m assignment1_m1_m2.milestone1_foundation
(run from the project root so the `common` package resolves)
================================================================================
"""

import sys
import os
import time
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.topology import build_nodes, NODE_SPECS
from common.node import start_node_process, wait_until_up
from common.communication import send_message

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)


def log(tag, msg):
    print(f"[{tag}] {msg}")


def main():
    log("M1", "Building topology: edge-1, edge-2, ran-1, core-1, cloud-1")
    nodes = build_nodes(packet_loss_rate=0.0)  # no loss yet - that's introduced in M2

    log("M1", "Launching each node as an independent OS process ...")
    handles = []
    for node in nodes:
        h = start_node_process(node)
        handles.append(h)
        log("M1", f"  -> {node.name:8s} ({node.node_type:5s}) pid={h.pid} listening on {node.host}:{node.port}")

    log("M1", "Waiting for all nodes to come up (inter-node communication check via 'ping') ...")
    up_status = {}
    for name, ntype, host, port, *_ in NODE_SPECS:
        ok = wait_until_up(host, port, timeout=10)
        up_status[name] = ok
        log("M1", f"  ping {name:8s} -> {'UP' if ok else 'FAILED'}")

    if not all(up_status.values()):
        log("M1", "ERROR: not all nodes came up. Aborting.")
        return

    # ---- [M1-b/c/d] Process creation + scheduling + resource allocation demo ----
    log("M1", "Submitting a small mixed-QoS workload to demonstrate scheduling + allocation ...")
    demo_tasks = [
        {"name": "urllc-control-loop", "service_class": "URLLC", "cpu": 1, "memory": 64,  "bandwidth": 5,  "work_units": 20},
        {"name": "embb-video-chunk",   "service_class": "eMBB",  "cpu": 2, "memory": 256, "bandwidth": 20, "work_units": 80},
        {"name": "mmtc-sensor-burst",  "service_class": "mMTC",  "cpu": 1, "memory": 32,  "bandwidth": 2,  "work_units": 30},
    ]
    target = ("edge-1", "127.0.0.1", 9101)
    results = []
    for task in demo_tasks:
        reply = send_message(target[1], target[2], {"type": "task_request", "task": task})
        log("M1", f"  sent {task['service_class']:5s} task '{task['name']}' to {target[0]} -> {reply.get('status')} "
                   f"(pid={reply.get('pid')}, rtt={reply.get('_rtt_ms')} ms)")
        results.append({"task": task, "reply": reply})

    # ---- [M1-e] Inter-node communication demo: Edge -> Core -> Cloud relay ----
    log("M1", "Demonstrating inter-node communication path: edge-1 -> core-1 -> cloud-1")
    for name, host, port in [("core-1", "127.0.0.1", 9301), ("cloud-1", "127.0.0.1", 9401)]:
        reply = send_message(host, port, {"type": "ping"})
        log("M1", f"  edge-1 reaches {name} directly: {reply.get('status')} (rtt={reply.get('_rtt_ms')} ms)")

    # ---- Snapshot process tables + resource state from every node ----
    system_state = {}
    for name, ntype, host, port, *_ in NODE_SPECS:
        pt = send_message(host, port, {"type": "process_table_request"})
        mx = send_message(host, port, {"type": "metrics_request"})
        system_state[name] = {
            "node_type": ntype,
            "process_table": pt.get("process_table", []),
            "metrics": mx,
        }

    out_path = os.path.join(RESULTS_DIR, "milestone1_system_state.json")
    with open(out_path, "w") as f:
        json.dump({"tasks_demo": results, "node_state": system_state}, f, indent=2, default=str)
    log("M1", f"System state (process tables, resource utilization) saved -> {out_path}")

    log("M1", "MILESTONE 1 COMPLETE. Nodes remain running in background for milestone2 script if launched next; "
               "otherwise terminate this process to stop them.")

    return nodes, handles


if __name__ == "__main__":
    main()
    # Keep alive briefly so a human/demo can see the servers are really up,
    # then exit (daemon child processes are torn down with the parent).
    time.sleep(2)
