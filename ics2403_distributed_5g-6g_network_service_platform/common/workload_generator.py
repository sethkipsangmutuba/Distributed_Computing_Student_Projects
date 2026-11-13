"""
workload_generator.py
----------------------
[MILESTONE 2 - Distributed Processing and Performance]

Generates a synthetic but telecom-realistic mix of service requests
representing the three canonical 5G/6G traffic classes:

    URLLC (Ultra-Reliable Low-Latency)  -> small, latency-critical
    eMBB  (Enhanced Mobile Broadband)   -> larger, bandwidth-heavy
    mMTC  (Massive Machine-Type Comms)  -> tiny, very frequent

Also provides simple placement policies used to DISTRIBUTE this
workload across the topology's nodes, which is what Milestone 2 asks
for ("distribute computation across multiple nodes").
"""

import random

SERVICE_PROFILES = {
    "URLLC": dict(cpu=(1, 2), memory=(32, 128), bandwidth=(2, 10), work_units=(5, 25)),
    "eMBB":  dict(cpu=(2, 4), memory=(128, 512), bandwidth=(10, 50), work_units=(40, 120)),
    "mMTC":  dict(cpu=(1, 1), memory=(8, 32), bandwidth=(1, 3), work_units=(5, 15)),
}

# Realistic-ish traffic mix: mMTC dominates by count, eMBB dominates by bytes.
CLASS_WEIGHTS = {"URLLC": 0.2, "eMBB": 0.3, "mMTC": 0.5}


def _sample_profile(service_class: str) -> dict:
    prof = SERVICE_PROFILES[service_class]
    return {
        "cpu": round(random.uniform(*prof["cpu"]), 2),
        "memory": round(random.uniform(*prof["memory"]), 1),
        "bandwidth": round(random.uniform(*prof["bandwidth"]), 1),
        "work_units": round(random.uniform(*prof["work_units"]), 1),
    }


def generate_workload(n_tasks: int, seed: int = 42) -> list:
    """Generates n_tasks synthetic telecom service requests."""
    rng = random.Random(seed)
    classes = list(CLASS_WEIGHTS.keys())
    weights = list(CLASS_WEIGHTS.values())
    tasks = []
    for i in range(n_tasks):
        svc = rng.choices(classes, weights=weights, k=1)[0]
        profile = {
            "cpu": round(rng.uniform(*SERVICE_PROFILES[svc]["cpu"]), 2),
            "memory": round(rng.uniform(*SERVICE_PROFILES[svc]["memory"]), 1),
            "bandwidth": round(rng.uniform(*SERVICE_PROFILES[svc]["bandwidth"]), 1),
            "work_units": round(rng.uniform(*SERVICE_PROFILES[svc]["work_units"]), 1),
        }
        tasks.append({
            "name": f"{svc.lower()}-task-{i}",
            "service_class": svc,
            **profile,
        })
    return tasks


def round_robin_placement(tasks: list, node_names: list) -> list:
    """Returns list of (task, node_name) pairs, round-robin across node_names."""
    return [(t, node_names[i % len(node_names)]) for i, t in enumerate(tasks)]


def edge_first_placement(tasks: list, edge_names: list, core_name: str, cloud_name: str) -> list:
    """
    A slightly smarter policy: small/latency-sensitive (URLLC) tasks stay
    at the edge; eMBB (bandwidth-heavy) goes to core; mMTC (many tiny
    tasks) is spread round-robin across edges to avoid overloading one.
    Used later as the 'baseline placement' to compare against the
    proposed improvement in Milestone 3/5.
    """
    placement = []
    edge_i = 0
    for t in tasks:
        if t["service_class"] == "URLLC":
            placement.append((t, edge_names[edge_i % len(edge_names)]))
            edge_i += 1
        elif t["service_class"] == "eMBB":
            placement.append((t, core_name))
        else:  # mMTC
            placement.append((t, edge_names[edge_i % len(edge_names)]))
            edge_i += 1
    return placement
