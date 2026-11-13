"""
lock_manager.py
----------------
[MILESTONE 6 - Concurrency and Deadlock Management]

A per-node, single-holder lock manager. In plain terms: each node has a
small set of "named resources" (e.g. "session_slot"). Only ONE
transaction can hold the lock on a given resource at a time -- this is
MUTUAL EXCLUSION. Anyone else who wants it has to WAIT.

Implemented as a short-interval polling wait (check every 50ms) rather
than condition-variable signaling. This is a deliberate simplicity
trade-off: it makes it trivial for an external deadlock DETECTOR to
forcibly abort a specific waiting transaction (mark_victim) without
needing any special wake-up signaling machinery -- the waiting thread
simply notices the victim flag on its next poll.

Every node's `snapshot()` exposes who holds what and who is waiting for
what; common/deadlock_detector.py polls this across ALL nodes to build
a system-wide wait-for graph (this is exactly "Model resource
allocation using P_i -> R_j" from the milestone brief).
"""

import time
import threading


class LockManager:
    def __init__(self):
        self._locks = {}      # resource_name -> tx_id currently holding it (or absent)
        self._waiters = {}    # resource_name -> [tx_id, tx_id, ...] waiting, in arrival order
        self._victims = set()  # tx_ids an external deadlock detector has condemned
        self._lock = threading.Lock()  # protects the three dicts/sets above

    def acquire(self, resource: str, tx_id: str, timeout: float = 15.0, poll_interval: float = 0.05) -> str:
        """
        Blocks (via polling) until the lock is granted, the caller is
        marked a deadlock victim, or `timeout` elapses.
        Returns one of: "granted", "aborted_by_deadlock_resolution", "timeout".
        """
        deadline = time.time() + timeout
        registered = False
        try:
            while time.time() < deadline:
                with self._lock:
                    if tx_id in self._victims:
                        self._victims.discard(tx_id)
                        return "aborted_by_deadlock_resolution"

                    holder = self._locks.get(resource)
                    if holder is None or holder == tx_id:
                        self._locks[resource] = tx_id
                        return "granted"

                    # Someone else holds it -> register as a waiter (this IS the
                    # "P_i is waiting for R_j" edge the deadlock detector reads).
                    self._waiters.setdefault(resource, [])
                    if tx_id not in self._waiters[resource]:
                        self._waiters[resource].append(tx_id)
                        registered = True
                time.sleep(poll_interval)
            return "timeout"
        finally:
            if registered:
                with self._lock:
                    lst = self._waiters.get(resource, [])
                    if tx_id in lst:
                        lst.remove(tx_id)

    def release(self, resource: str, tx_id: str):
        with self._lock:
            if self._locks.get(resource) == tx_id:
                del self._locks[resource]

    def mark_victim(self, tx_id: str):
        """Called by the deadlock detector: next time tx_id polls, it aborts instead of waiting."""
        with self._lock:
            self._victims.add(tx_id)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "locks": dict(self._locks),
                "waiters": {k: list(v) for k, v in self._waiters.items() if v},
            }
