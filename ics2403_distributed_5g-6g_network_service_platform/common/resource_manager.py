"""
resource_manager.py
--------------------
[MILESTONE 1 - Distributed OS Foundation]

Models the "Resources" part of the distributed system definition used
throughout this project:

        System = Nodes + Processes + Resources + Communication

Each node in the platform (Edge / RAN / Core / Cloud) owns a
ResourceManager that tracks how much CPU, memory and bandwidth it has,
and how much of that capacity is currently allocated to running
processes. This is intentionally simple (a capacity ledger, not a real
OS scheduler) because the goal at this stage is to demonstrate the
CONCEPT of distributed resource allocation, which later milestones
(M6 concurrency/deadlock, M8 model comparison) will build on.
"""

from dataclasses import dataclass, field
from threading import Lock
import time


@dataclass
class ResourceCapacity:
    cpu: float          # in "vCPU units"
    memory: float        # in MB
    bandwidth: float      # in Mbps


class InsufficientResourcesError(Exception):
    """Raised when a node cannot satisfy a process's resource request."""
    pass


class ResourceManager:
    """
    Tracks allocation of CPU / memory / bandwidth for a single node.
    Thread-safe because a node may receive concurrent task requests
    from multiple other nodes at once (relevant again in Milestone 6).
    """

    def __init__(self, cpu_capacity: float, memory_capacity: float, bandwidth_capacity: float):
        self.capacity = ResourceCapacity(cpu_capacity, memory_capacity, bandwidth_capacity)
        self.allocated = ResourceCapacity(0.0, 0.0, 0.0)
        self._lock = Lock()
        self.allocation_log = []  # (timestamp, process_id, action, cpu, mem, bw)

    def available(self) -> ResourceCapacity:
        return ResourceCapacity(
            cpu=self.capacity.cpu - self.allocated.cpu,
            memory=self.capacity.memory - self.allocated.memory,
            bandwidth=self.capacity.bandwidth - self.allocated.bandwidth,
        )

    def can_allocate(self, cpu: float, memory: float, bandwidth: float) -> bool:
        avail = self.available()
        return cpu <= avail.cpu and memory <= avail.memory and bandwidth <= avail.bandwidth

    def allocate(self, process_id: str, cpu: float, memory: float, bandwidth: float) -> bool:
        with self._lock:
            if not self.can_allocate(cpu, memory, bandwidth):
                self.allocation_log.append((time.time(), process_id, "DENIED", cpu, memory, bandwidth))
                return False
            self.allocated.cpu += cpu
            self.allocated.memory += memory
            self.allocated.bandwidth += bandwidth
            self.allocation_log.append((time.time(), process_id, "ALLOCATED", cpu, memory, bandwidth))
            return True

    def release(self, process_id: str, cpu: float, memory: float, bandwidth: float):
        with self._lock:
            self.allocated.cpu = max(0.0, self.allocated.cpu - cpu)
            self.allocated.memory = max(0.0, self.allocated.memory - memory)
            self.allocated.bandwidth = max(0.0, self.allocated.bandwidth - bandwidth)
            self.allocation_log.append((time.time(), process_id, "RELEASED", cpu, memory, bandwidth))

    def utilization(self) -> dict:
        """Returns utilization as a fraction (0.0 - 1.0) of each resource."""
        return {
            "cpu": round(self.allocated.cpu / self.capacity.cpu, 4) if self.capacity.cpu else 0,
            "memory": round(self.allocated.memory / self.capacity.memory, 4) if self.capacity.memory else 0,
            "bandwidth": round(self.allocated.bandwidth / self.capacity.bandwidth, 4) if self.capacity.bandwidth else 0,
        }

    def snapshot(self) -> dict:
        return {
            "capacity": vars(self.capacity),
            "allocated": vars(self.allocated),
            "utilization": self.utilization(),
        }
