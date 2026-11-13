"""
performance_monitor.py
-----------------------
[MILESTONE 2 - Distributed Processing and Performance]

Computes the quantitative measures the milestone requires:
    Throughput, Latency, Jitter, Packet loss, CPU utilization, Memory utilization
plus load-distribution analysis and bottleneck analysis across nodes.

    Throughput = Completed Work / Time
    Latency    = T_response - T_request
    Jitter     = variability (std. deviation) of latency across requests
"""

import statistics
import time


class RunRecorder:
    """Collects per-task client-side timing during a Milestone-2 run."""

    def __init__(self):
        self.records = []  # dict per task: node, service_class, status, latency_ms, t_request
        self.start_time = None
        self.end_time = None

    def start(self):
        self.start_time = time.time()

    def stop(self):
        self.end_time = time.time()

    def record(self, node_name, service_class, status, latency_ms):
        self.records.append({
            "node": node_name, "service_class": service_class,
            "status": status, "latency_ms": latency_ms,
        })

    # ---- aggregate metrics ----

    def wall_clock_seconds(self) -> float:
        return (self.end_time - self.start_time) if (self.start_time and self.end_time) else 0.0

    def completed_records(self):
        return [r for r in self.records if r["status"] == "completed"]

    def throughput(self) -> float:
        """Completed tasks per second over the whole run."""
        secs = self.wall_clock_seconds()
        completed = len(self.completed_records())
        return round(completed / secs, 3) if secs > 0 else 0.0

    def latency_stats(self) -> dict:
        lat = [r["latency_ms"] for r in self.completed_records()]
        if not lat:
            return {"mean_ms": 0, "min_ms": 0, "max_ms": 0, "p95_ms": 0, "jitter_ms": 0}
        lat_sorted = sorted(lat)
        p95_idx = min(len(lat_sorted) - 1, int(0.95 * len(lat_sorted)))
        return {
            "mean_ms": round(statistics.mean(lat), 3),
            "min_ms": round(min(lat), 3),
            "max_ms": round(max(lat), 3),
            "p95_ms": round(lat_sorted[p95_idx], 3),
            "jitter_ms": round(statistics.pstdev(lat), 3) if len(lat) > 1 else 0.0,
        }

    def packet_loss_rate(self) -> float:
        total = len(self.records)
        dropped = len([r for r in self.records if r["status"] in ("dropped", "rejected", "error")])
        return round(dropped / total, 4) if total else 0.0

    def status_breakdown(self) -> dict:
        breakdown = {}
        for r in self.records:
            breakdown[r["status"]] = breakdown.get(r["status"], 0) + 1
        return breakdown

    def load_distribution(self) -> dict:
        """How many tasks landed on each node (load-distribution analysis)."""
        dist = {}
        for r in self.records:
            dist[r["node"]] = dist.get(r["node"], 0) + 1
        return dist

    def per_node_latency(self) -> dict:
        by_node = {}
        for r in self.completed_records():
            by_node.setdefault(r["node"], []).append(r["latency_ms"])
        return {
            node: {
                "count": len(lats),
                "mean_ms": round(statistics.mean(lats), 3),
                "max_ms": round(max(lats), 3),
            } for node, lats in by_node.items()
        }

    def bottleneck_analysis(self, node_utilization: dict) -> dict:
        """
        Combines per-node latency + resource utilization snapshots
        (fetched from each node's metrics_request) to identify the
        weakest link in the pipeline.
        """
        per_node_lat = self.per_node_latency()
        candidates = []
        for node, lat_info in per_node_lat.items():
            util = node_utilization.get(node, {})
            cpu_u = util.get("resource_utilization", {}).get("cpu", 0)
            candidates.append({
                "node": node, "mean_latency_ms": lat_info["mean_ms"],
                "cpu_utilization": cpu_u,
                "score": lat_info["mean_ms"] * (1 + cpu_u),  # simple composite bottleneck score
            })
        candidates.sort(key=lambda c: c["score"], reverse=True)
        return {
            "ranked_nodes_worst_first": candidates,
            "likely_bottleneck": candidates[0]["node"] if candidates else None,
        }

    def summary(self, node_utilization: dict = None) -> dict:
        return {
            "wall_clock_seconds": round(self.wall_clock_seconds(), 3),
            "total_requests": len(self.records),
            "throughput_tasks_per_sec": self.throughput(),
            "latency": self.latency_stats(),
            "packet_loss_rate": self.packet_loss_rate(),
            "status_breakdown": self.status_breakdown(),
            "load_distribution": self.load_distribution(),
            "per_node_latency": self.per_node_latency(),
            "bottleneck_analysis": self.bottleneck_analysis(node_utilization or {}),
        }
