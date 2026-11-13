"""
================================================================================
ASSIGNMENT 1 | MILESTONE 2 - Distributed Processing and Performance (Week 2)
================================================================================
System: Distributed 5G/6G Network Service Platform (Theme 1)

WHAT THIS SCRIPT DEMONSTRATES (per the milestone brief):
  [M2-a] Distributing computation (a generated telecom workload) across
         the SAME multi-node platform built in Milestone 1
  [M2-b] Measuring: Throughput, Latency, Jitter, Packet loss,
         CPU utilization, Memory utilization
  [M2-c] Load-distribution analysis (tasks per node)
  [M2-d] Bottleneck analysis (which node is the weakest link)

This script starts its own fresh copy of the topology (independent of
milestone1_foundation.py) so it can be run standalone, but reuses
EXACTLY the same common/ modules -- this is the "one evolving system"
requirement, not a new prototype.

Run:
    python3 -m assignment1_m1_m2.milestone2_processing_performance
================================================================================
"""

import sys
import os
import json
import concurrent.futures

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.topology import build_nodes, NODE_SPECS
from common.node import start_node_process, wait_until_up
from common.communication import send_message
from common.workload_generator import generate_workload, edge_first_placement
from common.performance_monitor import RunRecorder

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

NODE_ADDR = {name: (host, port) for name, ntype, host, port, *_ in NODE_SPECS}


def log(tag, msg):
    print(f"[{tag}] {msg}")


def submit_task(node_name, task):
    host, port = NODE_ADDR[node_name]
    try:
        reply = send_message(host, port, {"type": "task_request", "task": task}, timeout=5.0)
        status = reply.get("status", "error")
        latency = reply.get("server_latency_ms", reply.get("_rtt_ms", 0.0))
        return node_name, task["service_class"], status, latency
    except Exception as e:
        return node_name, task["service_class"], "error", 0.0


def main():
    N_TASKS = 120
    CONCURRENCY = 6

    log("M2", f"Building topology with a small simulated link packet-loss rate (2%) to exercise reliability metrics")
    nodes = build_nodes(packet_loss_rate=0.02)

    log("M2", "Launching nodes as independent OS processes ...")
    for node in nodes:
        start_node_process(node)
    for name, ntype, host, port, *_ in NODE_SPECS:
        ok = wait_until_up(host, port, timeout=10)
        log("M2", f"  {name:8s} up: {ok}")

    log("M2", f"Generating synthetic telecom workload: {N_TASKS} tasks (URLLC / eMBB / mMTC mix)")
    tasks = generate_workload(N_TASKS, seed=7)
    class_counts = {}
    for t in tasks:
        class_counts[t["service_class"]] = class_counts.get(t["service_class"], 0) + 1
    log("M2", f"  workload composition: {class_counts}")

    edge_names = ["edge-1", "edge-2"]
    placement = edge_first_placement(tasks, edge_names, core_name="ran-1", cloud_name="cloud-1")
    log("M2", "Placement policy: edge-first (URLLC/mMTC -> nearest edge round-robin, eMBB -> ran-1)")

    recorder = RunRecorder()
    log("M2", f"Distributing {N_TASKS} tasks across the platform with concurrency={CONCURRENCY} ...")
    recorder.start()
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = [pool.submit(submit_task, node_name, task) for task, node_name in placement]
        for fut in concurrent.futures.as_completed(futures):
            node_name, svc, status, latency = fut.result()
            recorder.record(node_name, svc, status, latency)
    recorder.stop()
    log("M2", "Workload distribution complete.")

    # ---- Pull CPU / memory utilization snapshots from every node ----
    log("M2", "Collecting per-node resource utilization (CPU / memory / bandwidth) ...")
    node_utilization = {}
    for name, ntype, host, port, *_ in NODE_SPECS:
        mx = send_message(host, port, {"type": "metrics_request"})
        node_utilization[name] = mx
        util = mx.get("resource_utilization", {})
        log("M2", f"  {name:8s} cpu={util.get('cpu',0)*100:5.1f}%  mem={util.get('memory',0)*100:5.1f}%  "
                   f"bw={util.get('bandwidth',0)*100:5.1f}%  completed={mx.get('tasks_completed')}  "
                   f"dropped={mx.get('tasks_dropped')}  rejected={mx.get('tasks_rejected_no_resources')}")

    summary = recorder.summary(node_utilization)

    log("M2", "---- PERFORMANCE SUMMARY ----")
    log("M2", f"  wall clock:      {summary['wall_clock_seconds']} s")
    log("M2", f"  throughput:      {summary['throughput_tasks_per_sec']} tasks/sec")
    log("M2", f"  latency (mean):  {summary['latency']['mean_ms']} ms   (p95={summary['latency']['p95_ms']} ms)")
    log("M2", f"  jitter (stdev):  {summary['latency']['jitter_ms']} ms")
    log("M2", f"  packet loss:     {summary['packet_loss_rate']*100:.2f}%")
    log("M2", f"  load distribution: {summary['load_distribution']}")
    log("M2", f"  likely bottleneck node: {summary['bottleneck_analysis']['likely_bottleneck']}")

    out_path = os.path.join(RESULTS_DIR, "milestone2_performance_summary.json")
    with open(out_path, "w") as f:
        json.dump({
            "n_tasks": N_TASKS, "concurrency": CONCURRENCY, "workload_composition": class_counts,
            "summary": summary, "node_utilization_raw": node_utilization,
        }, f, indent=2, default=str)
    log("M2", f"Full results saved -> {out_path}")
    log("M2", "MILESTONE 2 COMPLETE.")

    return summary


if __name__ == "__main__":
    main()
