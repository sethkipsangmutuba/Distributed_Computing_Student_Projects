"""
================================================================================
ASSIGNMENT 2 | MILESTONE 3 - Distributed Architecture (Week 3)
================================================================================
System: Distributed 5G/6G Network Service Platform (Theme 1)

WHAT THIS SCRIPT DEMONSTRATES (per the milestone brief):
  [M3-a] Service decomposition: the platform is shown to be a
         SERVICE-ORIENTED / MICROSERVICE architecture, not a monolith
         -- each node advertises the distinct services it hosts
         (see common/topology.py::SERVICES_BY_TYPE).
  [M3-b] Deployment architecture: the Edge <-> RAN <-> Core <-> Cloud
         layering already built in M1/M2 is queried and rendered
         explicitly as the deployment topology.
  [M3-c] Scalability analysis: the SAME M2 workload-distribution
         harness is re-run while scaling the number of Edge nodes
         (1 -> 2 -> 4), measuring how throughput/latency respond.
  [M3-d] Edge vs Core trade-off analysis: the identical workload is
         run twice with different placement policies -- "edge-only"
         (everything handled locally at the edge) vs "core-routed"
         (everything forwarded to the core) -- to quantify the
         latency/capacity trade-off between the two architectural
         choices.

This reuses common/node.py, common/topology.py, common/workload_generator.py
and common/performance_monitor.py UNCHANGED from Assignment 1 -- Milestone 3
is architectural REASONING about the existing system, plus new experiments,
not a rebuild.

Run:
    python -m assignment2_m3_m4.milestone3_architecture
================================================================================
"""

import sys
import os
import json
import time
import concurrent.futures
from multiprocessing import Process as OSProcess

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.node import Node, start_node_process, wait_until_up, _node_entry
from common.topology import NodeConfig, SERVICES_BY_TYPE
from common.communication import send_message
from common.workload_generator import generate_workload
from common.performance_monitor import RunRecorder

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)


def log(tag, msg):
    print(f"[{tag}] {msg}")


# ---------------------------------------------------------------------------
# [M3-a/b] Service decomposition + deployment architecture
# ---------------------------------------------------------------------------

STANDARD_CONFIGS = [
    NodeConfig("edge-1", "EDGE", "127.0.0.1", 9101, 8, 3072, 150, "priority", 0.0, 10, SERVICES_BY_TYPE["EDGE"]),
    NodeConfig("edge-2", "EDGE", "127.0.0.1", 9102, 8, 3072, 150, "priority", 0.0, 11, SERVICES_BY_TYPE["EDGE"]),
    NodeConfig("ran-1", "RAN", "127.0.0.1", 9201, 12, 6144, 500, "priority", 0.0, 20, SERVICES_BY_TYPE["RAN"]),
    NodeConfig("core-1", "CORE", "127.0.0.1", 9301, 16, 8192, 1000, "priority", 0.0, 30, SERVICES_BY_TYPE["CORE"]),
    NodeConfig("cloud-1", "CLOUD", "127.0.0.1", 9401, 32, 16384, 5000, "priority", 0.0, 40, SERVICES_BY_TYPE["CLOUD"]),
]


def launch(configs):
    for c in configs:
        start_node_process(c)
    for c in configs:
        wait_until_up(c.host, c.port, timeout=10)


def demonstrate_service_decomposition():
    log("M3", "Querying every node's advertised services (service-oriented decomposition) ...")
    architecture = {}
    for c in STANDARD_CONFIGS:
        reply = send_message(c.host, c.port, {"type": "service_info"})
        architecture[c.name] = {"node_type": c.node_type, "services": reply.get("services", [])}
        log("M3", f"  {c.name:8s} [{c.node_type:5s}] hosts: {reply.get('services')}")
    return architecture


# ---------------------------------------------------------------------------
# [M3-c] Scalability analysis: scale the number of Edge nodes
# ---------------------------------------------------------------------------

def run_workload_against(node_addrs, n_tasks=90, concurrency=6):
    """
    node_addrs: list of (name, host, port) to round-robin the workload across.
    Returns a RunRecorder summary dict.
    """
    tasks = generate_workload(n_tasks, seed=11)
    placement = [(t, node_addrs[i % len(node_addrs)]) for i, t in enumerate(tasks)]

    recorder = RunRecorder()
    recorder.start()

    def submit(task, addr):
        name, host, port = addr
        try:
            reply = send_message(host, port, {"type": "task_request", "task": task}, timeout=5.0)
            status = reply.get("status", "error")
            latency = reply.get("server_latency_ms", reply.get("_rtt_ms", 0.0))
            return name, task["service_class"], status, latency
        except Exception:
            return name, task["service_class"], "error", 0.0

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(submit, task, addr) for task, addr in placement]
        for fut in concurrent.futures.as_completed(futures):
            name, svc, status, latency = fut.result()
            recorder.record(name, svc, status, latency)
    recorder.stop()
    return recorder.summary()


def scalability_experiment():
    """
    Scales the number of EDGE nodes handling the same total workload:
    1 edge node -> 2 edge nodes -> 4 edge nodes (2 extra spun up just
    for this experiment), holding workload size fixed, to see how
    throughput and latency respond to horizontal scaling.
    """
    log("M3", "Scalability experiment: same workload, increasing number of edge nodes ...")

    extra_configs = [
        NodeConfig("edge-3", "EDGE", "127.0.0.1", 9103, 8, 3072, 150, "priority", 0.0, 12, SERVICES_BY_TYPE["EDGE"]),
        NodeConfig("edge-4", "EDGE", "127.0.0.1", 9104, 8, 3072, 150, "priority", 0.0, 13, SERVICES_BY_TYPE["EDGE"]),
    ]
    for c in extra_configs:
        start_node_process(c)
        wait_until_up(c.host, c.port, timeout=10)

    edge_pool = [("edge-1", "127.0.0.1", 9101), ("edge-2", "127.0.0.1", 9102),
                 ("edge-3", "127.0.0.1", 9103), ("edge-4", "127.0.0.1", 9104)]

    results = {}
    for n_edges in (1, 2, 4):
        addrs = edge_pool[:n_edges]
        summary = run_workload_against(addrs, n_tasks=90, concurrency=6)
        results[f"{n_edges}_edge_nodes"] = {
            "throughput_tasks_per_sec": summary["throughput_tasks_per_sec"],
            "mean_latency_ms": summary["latency"]["mean_ms"],
            "p95_latency_ms": summary["latency"]["p95_ms"],
            "packet_loss_rate": summary["packet_loss_rate"],
        }
        log("M3", f"  {n_edges} edge node(s): throughput={summary['throughput_tasks_per_sec']} tasks/s, "
                   f"mean_latency={summary['latency']['mean_ms']} ms, loss={summary['packet_loss_rate']*100:.1f}%")
    return results


# ---------------------------------------------------------------------------
# [M3-d] Edge vs Core trade-off analysis
# ---------------------------------------------------------------------------

def edge_vs_core_tradeoff():
    log("M3", "Edge-vs-Core trade-off: identical workload, two placement extremes ...")

    edge_only_summary = run_workload_against(
        [("edge-1", "127.0.0.1", 9101), ("edge-2", "127.0.0.1", 9102)], n_tasks=90, concurrency=6)
    core_only_summary = run_workload_against(
        [("core-1", "127.0.0.1", 9301)], n_tasks=90, concurrency=6)

    result = {
        "edge_only": {
            "throughput_tasks_per_sec": edge_only_summary["throughput_tasks_per_sec"],
            "mean_latency_ms": edge_only_summary["latency"]["mean_ms"],
            "packet_loss_rate": edge_only_summary["packet_loss_rate"],
        },
        "core_only": {
            "throughput_tasks_per_sec": core_only_summary["throughput_tasks_per_sec"],
            "mean_latency_ms": core_only_summary["latency"]["mean_ms"],
            "packet_loss_rate": core_only_summary["packet_loss_rate"],
        },
    }
    log("M3", f"  edge-only : throughput={result['edge_only']['throughput_tasks_per_sec']} tasks/s, "
               f"latency={result['edge_only']['mean_latency_ms']} ms, loss={result['edge_only']['packet_loss_rate']*100:.1f}%")
    log("M3", f"  core-only : throughput={result['core_only']['throughput_tasks_per_sec']} tasks/s, "
               f"latency={result['core_only']['mean_latency_ms']} ms, loss={result['core_only']['packet_loss_rate']*100:.1f}%")
    log("M3", "  Interpretation: edge nodes are individually weaker but there are MORE of them and they are "
               "closer to the user; core-1 is a single, larger, but more distant resource. Real 5G/6G "
               "placement should split load by QoS requirement (this motivates Milestone 5's proposed "
               "edge-aware placement policy).")
    return result


def main():
    log("M3", "Launching the standard 5-node platform (same topology as Assignment 1) ...")
    launch(STANDARD_CONFIGS)

    architecture = demonstrate_service_decomposition()
    scalability = scalability_experiment()
    tradeoff = edge_vs_core_tradeoff()

    out_path = os.path.join(RESULTS_DIR, "milestone3_architecture.json")
    with open(out_path, "w") as f:
        json.dump({
            "service_decomposition": architecture,
            "scalability_experiment": scalability,
            "edge_vs_core_tradeoff": tradeoff,
        }, f, indent=2, default=str)
    log("M3", f"Results saved -> {out_path}")
    log("M3", "MILESTONE 3 COMPLETE.")


if __name__ == "__main__":
    main()
