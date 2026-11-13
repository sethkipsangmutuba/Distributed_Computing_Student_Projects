"""
process_manager.py
-------------------
[MILESTONE 1 - Distributed OS Foundation]

Models the "Processes" part of:  System = Nodes + Processes + Resources + Communication

A Process here represents a unit of work submitted to a node in the
5G/6G platform - e.g. "handle an eMBB video session", "run a URLLC
control-loop task", "process an mMTC sensor burst". Each process has
a resource footprint (cpu/memory/bandwidth) and a lifecycle:

    NEW -> READY -> RUNNING -> TERMINATED
                          \\-> FAILED

The ProcessManager owned by each node is responsible for creating
processes and scheduling them onto the node's ResourceManager.
Two scheduling policies are implemented to allow later comparison:
FIFO and Priority (URLLC traffic gets priority over eMBB/mMTC, which
mirrors real 5G/6G QoS classes).
"""

from dataclasses import dataclass, field
from enum import Enum
import itertools
import time
import heapq
from threading import Lock


class ProcessState(Enum):
    NEW = "NEW"
    READY = "READY"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    FAILED = "FAILED"


_pid_counter = itertools.count(1)


@dataclass
class Process:
    name: str
    service_class: str          # "URLLC" | "eMBB" | "mMTC"
    cpu_required: float
    memory_required: float
    bandwidth_required: float
    work_units: float            # amount of "work" -> determines execution time
    pid: int = field(default_factory=lambda: next(_pid_counter))
    state: ProcessState = ProcessState.NEW
    created_at: float = field(default_factory=time.time)
    started_at: float = None
    finished_at: float = None

    # URLLC = highest priority (0), eMBB = 1, mMTC = 2 (lowest)
    PRIORITY_MAP = {"URLLC": 0, "eMBB": 1, "mMTC": 2}

    @property
    def priority(self):
        return self.PRIORITY_MAP.get(self.service_class, 3)

    def to_dict(self):
        d = {
            "pid": self.pid, "name": self.name, "service_class": self.service_class,
            "cpu_required": self.cpu_required, "memory_required": self.memory_required,
            "bandwidth_required": self.bandwidth_required, "work_units": self.work_units,
            "state": self.state.value, "created_at": self.created_at,
            "started_at": self.started_at, "finished_at": self.finished_at,
        }
        return d


class ProcessManager:
    """
    Owned by a single node. Creates processes and schedules them
    (FIFO or Priority) against that node's ResourceManager.
    """

    def __init__(self, node_name: str, resource_manager, policy: str = "priority"):
        self.node_name = node_name
        self.resource_manager = resource_manager
        self.policy = policy  # "fifo" or "priority"
        self._fifo_queue = []
        self._priority_queue = []   # heap of (priority, seq, process)
        self._seq = itertools.count()
        self.all_processes = {}
        self._lock = Lock()

    def create_process(self, name, service_class, cpu, memory, bandwidth, work_units) -> Process:
        p = Process(name=name, service_class=service_class, cpu_required=cpu,
                    memory_required=memory, bandwidth_required=bandwidth, work_units=work_units)
        p.state = ProcessState.READY
        with self._lock:
            self.all_processes[p.pid] = p
            if self.policy == "priority":
                heapq.heappush(self._priority_queue, (p.priority, next(self._seq), p))
            else:
                self._fifo_queue.append(p)
        return p

    def _next_ready_process(self):
        with self._lock:
            if self.policy == "priority":
                if self._priority_queue:
                    return heapq.heappop(self._priority_queue)[2]
                return None
            else:
                if self._fifo_queue:
                    return self._fifo_queue.pop(0)
                return None

    def dispatch_next(self):
        """
        Pop the next process per scheduling policy, try to allocate
        resources for it. Returns (process, allocated: bool).
        """
        p = self._next_ready_process()
        if p is None:
            return None, False
        ok = self.resource_manager.allocate(str(p.pid), p.cpu_required, p.memory_required, p.bandwidth_required)
        if ok:
            p.state = ProcessState.RUNNING
            p.started_at = time.time()
        else:
            p.state = ProcessState.FAILED
        return p, ok

    def terminate(self, process: Process):
        self.resource_manager.release(str(process.pid), process.cpu_required,
                                       process.memory_required, process.bandwidth_required)
        process.state = ProcessState.TERMINATED
        process.finished_at = time.time()

    def process_table(self):
        return [p.to_dict() for p in self.all_processes.values()]
