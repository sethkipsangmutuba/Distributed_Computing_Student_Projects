"""
deadlock_detector.py
---------------------
[MILESTONE 6 - Concurrency and Deadlock Management]

A DEADLOCK happens when transaction A is waiting for a resource held by
transaction B, while B is (directly or indirectly) waiting for a
resource held by A -- a circular wait, with nobody able to proceed.

This module implements DETECTION + RECOVERY (as opposed to prevention):
  1. DETECT: poll every node's lock_manager snapshot (who holds what,
     who is waiting for what), and merge them into ONE system-wide
     wait-for graph: an edge P_i -> P_j means "process/transaction P_i
     is waiting for a resource currently held by P_j".
  2. Find cycles in that graph (a cycle = a deadlock).
  3. RECOVER: for each cycle found, pick a "victim" transaction and
     force-abort it (mark_victim on whichever node it's waiting at),
     which releases the resource it was already holding and lets the
     rest of the cycle proceed.
"""

import time


class DeadlockDetector:
    def __init__(self, node_addrs: dict, send_fn):
        """node_addrs: name -> (host, port). send_fn: common.communication.send_message"""
        self.node_addrs = node_addrs
        self.send_fn = send_fn

    def snapshot_all_nodes(self) -> dict:
        """Returns {node_name: {"locks": {...}, "waiters": {...}}} for every node."""
        snapshots = {}
        for name, (host, port) in self.node_addrs.items():
            try:
                snapshots[name] = self.send_fn(host, port, {"type": "lock_state_request"}, timeout=2.0)
            except Exception:
                snapshots[name] = {"locks": {}, "waiters": {}}
        return snapshots

    def build_wait_for_graph(self, snapshots: dict) -> dict:
        """
        Merges every node's local lock state into ONE global wait-for
        graph: {tx_id: set(tx_ids it is waiting behind)}.
        This is the "P_i -> R_j -> (held by) P_k" chain collapsed
        directly into a P_i -> P_k edge, per node, then unioned.
        """
        graph = {}
        for node_name, snap in snapshots.items():
            locks = snap.get("locks", {})
            waiters = snap.get("waiters", {})
            for resource, holder in locks.items():
                for waiter in waiters.get(resource, []):
                    if waiter == holder:
                        continue
                    graph.setdefault(waiter, set()).add(holder)
        return graph

    def find_cycles(self, graph: dict) -> list:
        """Simple DFS-based cycle detection. Returns a list of cycles (each a list of tx_ids)."""
        visited = set()
        cycles = []

        def dfs(node, path):
            if node in path:
                idx = path.index(node)
                cycle = path[idx:] + [node]
                if cycle not in cycles:
                    cycles.append(cycle)
                return
            if node in visited:
                return
            visited.add(node)
            for nxt in graph.get(node, ()):
                dfs(nxt, path + [node])

        for n in list(graph.keys()):
            dfs(n, [])
        return cycles

    def resolve_cycle(self, cycle: list, snapshots: dict) -> dict:
        """
        Picks the victim (simplest deterministic policy: the
        lexicographically-largest tx_id in the cycle, i.e. treat later
        transaction IDs as "younger" and sacrifice the youngest -- a
        common, simple deadlock-victim heuristic) and broadcasts
        mark_victim to every node (harmless no-op wherever that tx_id
        isn't actually waiting).
        """
        victim = max(set(cycle))
        results = {}
        for name, (host, port) in self.node_addrs.items():
            try:
                reply = self.send_fn(host, port, {"type": "lock_mark_victim", "tx_id": victim}, timeout=2.0)
                results[name] = reply.get("status")
            except Exception:
                results[name] = "unreachable"
        return {"victim": victim, "cycle": cycle, "broadcast_results": results}

    def run_detection_cycle(self) -> dict:
        """One full detect (+ resolve if found) pass. Returns a report dict."""
        t0 = time.time()
        snapshots = self.snapshot_all_nodes()
        graph = self.build_wait_for_graph(snapshots)
        cycles = self.find_cycles(graph)
        resolutions = [self.resolve_cycle(c, snapshots) for c in cycles]
        return {
            "wait_for_graph": {k: list(v) for k, v in graph.items()},
            "cycles_found": cycles,
            "resolutions": resolutions,
            "detection_elapsed_ms": round((time.time() - t0) * 1000.0, 3),
        }
