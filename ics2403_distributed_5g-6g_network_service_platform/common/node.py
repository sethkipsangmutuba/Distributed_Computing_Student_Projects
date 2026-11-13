"""
node.py
-------
[MILESTONE 1 - Distributed OS Foundation]
[MILESTONE 2 - Distributed Processing and Performance] (execution + metrics)
[MILESTONE 3 - Distributed Architecture] (service decomposition metadata)
[MILESTONE 4 - Distributed Algorithms and Coordination] (Lamport clock + Bully election)
[MILESTONE 5 - Distributed Transactions] (2PC participant: prepare/commit/abort)
[MILESTONE 6 - Concurrency and Deadlock Management] (per-node lock manager)

Defines Node, the fundamental unit of the Distributed 5G/6G Network
Service Platform. Each Node:

  - Runs as its OWN operating-system process (multiprocessing.Process),
    demonstrating real process-level distribution rather than a single
    program simulating multiple "virtual" nodes in-memory.
  - Owns a ResourceManager (CPU/memory/bandwidth) and a ProcessManager
    (process creation + scheduling).
  - Hosts a MessageServer (TCP) so other nodes / the orchestrator can
    submit work to it -> the basic inter-node communication mechanism.
  - Simulates task EXECUTION by holding resources for a duration
    proportional to the task's work_units (this is what lets Milestone
    2 measure throughput, latency and utilization meaningfully).
  - Tracks lightweight metrics (tasks completed, tasks dropped/failed,
    per-task latency) used directly by Milestone 2's performance
    monitor.

Node types in Theme 1 (Distributed 5G/6G Network Service Platform):
    EDGE, RAN (radio/access network), CORE, CLOUD
"""

import time
import random
from multiprocessing import Process as OSProcess

from common.resource_manager import ResourceManager
from common.process_manager import ProcessManager
from common.communication import MessageServer, send_message
from common.coordination import LamportClock
from common.lock_manager import LockManager


class Node:
    def __init__(self, name: str, node_type: str, host: str, port: int,
                 cpu_capacity: float, memory_capacity: float, bandwidth_capacity: float,
                 scheduling_policy: str = "priority", packet_loss_rate: float = 0.0,
                 election_id: int = 0, services: tuple = ()):
        self.name = name
        self.node_type = node_type  # EDGE | RAN | CORE | CLOUD
        self.host = host
        self.port = port
        self.resource_manager = ResourceManager(cpu_capacity, memory_capacity, bandwidth_capacity)
        self.process_manager = ProcessManager(name, self.resource_manager, policy=scheduling_policy)
        self.packet_loss_rate = packet_loss_rate  # simulated link unreliability (Milestone 2)
        self.election_id = election_id            # [M4] rank for Bully leader election
        self.services = services                   # [M3] microservices this node hosts
        self.lamport_clock = LamportClock()         # [M4] logical clock, ticks on every message
        self.current_leader = None                  # [M4] last-known elected coordinator
        self.lock_manager = LockManager()            # [M6] per-node resource lock table
        self.transaction_log = {}                     # [M5] tx_id -> {"state":..., "reservation":...}
        self.metrics = {
            "tasks_received": 0,
            "tasks_completed": 0,
            "tasks_dropped": 0,
            "tasks_rejected_no_resources": 0,
            "task_latencies_ms": [],
        }
        self._server = None

    # ---------- message handling ----------

    def _handle_message(self, message: dict) -> dict:
        msg_type = message.get("type")

        # [M4] Every inbound message advances this node's Lamport clock per
        # Lamport's rule (local_clock = max(local, received) + 1), giving a
        # consistent logical event order across the whole distributed system.
        self.lamport_clock.update(message.get("lamport_ts", 0))

        if msg_type == "ping":
            return {"status": "ok", "node": self.name, "node_type": self.node_type,
                    "services": list(self.services), "lamport_ts": self.lamport_clock.value}

        if msg_type == "task_request":
            return self._handle_task_request(message)

        if msg_type == "metrics_request":
            return self._metrics_snapshot()

        if msg_type == "process_table_request":
            return {"status": "ok", "process_table": self.process_manager.process_table()}

        if msg_type == "service_info":
            return {"status": "ok", "node": self.name, "node_type": self.node_type,
                    "services": list(self.services)}

        # [M4] Bully leader election: a peer with a lower election_id is
        # asking "are you alive and higher-ranked?" -- since it only
        # contacts higher-ranked peers, replying at all means "yes, back off".
        if msg_type == "election":
            return {"status": "ok", "node": self.name, "election_id": self.election_id,
                    "lamport_ts": self.lamport_clock.value}

        # [M4] A peer has won an election and is announcing itself as leader.
        if msg_type == "coordinator":
            self.current_leader = message.get("leader")
            return {"status": "ack", "node": self.name, "acknowledged_leader": self.current_leader,
                    "lamport_ts": self.lamport_clock.value}

        if msg_type == "leader_query":
            return {"status": "ok", "node": self.name, "current_leader": self.current_leader,
                    "lamport_ts": self.lamport_clock.value}

        # ---- [MILESTONE 5] Two-Phase Commit participant side ----
        if msg_type == "tx_prepare":
            return self._handle_tx_prepare(message)
        if msg_type == "tx_commit":
            return self._handle_tx_commit(message)
        if msg_type == "tx_abort":
            return self._handle_tx_abort(message)
        if msg_type == "tx_release":
            return self._handle_tx_release(message)

        # ---- [MILESTONE 6] Distributed lock manager ----
        if msg_type == "lock_acquire":
            result = self.lock_manager.acquire(message["resource"], message["tx_id"],
                                                timeout=message.get("timeout", 15.0))
            return {"status": result, "node": self.name}
        if msg_type == "lock_release":
            self.lock_manager.release(message["resource"], message["tx_id"])
            return {"status": "released", "node": self.name}
        if msg_type == "lock_state_request":
            return {"status": "ok", "node": self.name, **self.lock_manager.snapshot()}
        if msg_type == "lock_mark_victim":
            self.lock_manager.mark_victim(message["tx_id"])
            return {"status": "marked", "node": self.name}

        return {"status": "error", "error": f"unknown message type '{msg_type}'"}

    # ---------- [MILESTONE 5] Two-Phase Commit participant handlers ----------

    def _handle_tx_prepare(self, message: dict) -> dict:
        """
        PREPARE: "could you do this reservation, if asked?" We check
        (via ResourceManager.can_allocate) but do NOT actually allocate
        yet -- that only happens on tx_commit. This is what makes the
        transaction ATOMIC: nothing real changes until every participant
        has already agreed it CAN.
        """
        tx_id = message["tx_id"]
        r = message["reservation"]
        can_do = self.resource_manager.can_allocate(r["cpu"], r["memory"], r["bandwidth"])
        self.transaction_log[tx_id] = {
            "state": "prepared" if can_do else "vote_abort", "reservation": r,
        }
        return {"status": "ok", "node": self.name, "vote": "commit" if can_do else "abort"}

    def _handle_tx_commit(self, message: dict) -> dict:
        """COMMIT: the coordinator confirmed every participant voted commit -> actually apply it now."""
        tx_id = message["tx_id"]
        entry = self.transaction_log.get(tx_id)
        if entry is None or entry["state"] != "prepared":
            return {"status": "error", "node": self.name, "error": "no such prepared transaction"}
        r = entry["reservation"]
        ok = self.resource_manager.allocate(tx_id, r["cpu"], r["memory"], r["bandwidth"])
        entry["state"] = "committed" if ok else "commit_failed"
        return {"status": "committed" if ok else "commit_failed", "node": self.name}

    def _handle_tx_abort(self, message: dict) -> dict:
        """ABORT: at least one participant said no (or was unreachable) -> everyone discards the reservation."""
        tx_id = message["tx_id"]
        entry = self.transaction_log.get(tx_id, {"state": "aborted", "reservation": {}})
        entry["state"] = "aborted"
        self.transaction_log[tx_id] = entry
        return {"status": "aborted", "node": self.name}

    def _handle_tx_release(self, message: dict) -> dict:
        """
        RELEASE (session teardown): frees resources held by a previously
        COMMITTED transaction. Not part of the formal 2PC vote -- this
        models the end of a session's lifetime (establish -> use ->
        teardown), so a stream of transactions doesn't permanently
        accumulate resource usage forever.
        """
        tx_id = message["tx_id"]
        entry = self.transaction_log.get(tx_id)
        if entry is None or entry["state"] != "committed":
            return {"status": "error", "node": self.name, "error": "no committed transaction to release"}
        r = entry["reservation"]
        self.resource_manager.release(tx_id, r["cpu"], r["memory"], r["bandwidth"])
        entry["state"] = "released"
        return {"status": "released", "node": self.name}

    def _handle_task_request(self, message: dict) -> dict:
        t_recv = time.time()
        self.metrics["tasks_received"] += 1

        # Simulated packet loss on this node's inbound link (Milestone 2 reliability metric)
        if random.random() < self.packet_loss_rate:
            self.metrics["tasks_dropped"] += 1
            return {"status": "dropped", "node": self.name}

        task = message["task"]
        proc = self.process_manager.create_process(
            name=task["name"], service_class=task.get("service_class", "eMBB"),
            cpu=task["cpu"], memory=task["memory"], bandwidth=task["bandwidth"],
            work_units=task["work_units"],
        )

        proc, allocated = self.process_manager.dispatch_next()
        if not allocated:
            self.metrics["tasks_rejected_no_resources"] += 1
            return {"status": "rejected", "reason": "insufficient_resources", "node": self.name,
                     "pid": proc.pid if proc else None}

        # Simulate execution time: work_units translated into real (short) sleep,
        # scaled down so the whole distributed system runs quickly for grading/demo.
        exec_seconds = proc.work_units / 1000.0
        time.sleep(exec_seconds)

        self.process_manager.terminate(proc)
        self.metrics["tasks_completed"] += 1
        latency_ms = (time.time() - t_recv) * 1000.0
        self.metrics["task_latencies_ms"].append(latency_ms)

        return {
            "status": "completed", "node": self.name, "node_type": self.node_type,
            "pid": proc.pid, "server_latency_ms": round(latency_ms, 3),
        }

    def _metrics_snapshot(self) -> dict:
        lat = self.metrics["task_latencies_ms"]
        return {
            "status": "ok",
            "node": self.name,
            "node_type": self.node_type,
            "tasks_received": self.metrics["tasks_received"],
            "tasks_completed": self.metrics["tasks_completed"],
            "tasks_dropped": self.metrics["tasks_dropped"],
            "tasks_rejected_no_resources": self.metrics["tasks_rejected_no_resources"],
            "avg_latency_ms": round(sum(lat) / len(lat), 3) if lat else 0.0,
            "resource_utilization": self.resource_manager.utilization(),
            "current_leader": self.current_leader,
            "lamport_ts": self.lamport_clock.value,
            "transactions_seen": len(self.transaction_log),
            "lock_state": self.lock_manager.snapshot(),
        }

    # ---------- lifecycle ----------

    def run_forever(self):
        """Entry point when the node is launched as its own OS process."""
        self._server = MessageServer(self.host, self.port, self._handle_message)
        self._server.start()
        while True:
            time.sleep(1)


def _node_entry(config):
    """
    Module-level target function for multiprocessing.Process.

    Windows (and any platform using the 'spawn' start method) pickles
    the target + its arguments to hand off to the new process. `config`
    is a plain NodeConfig (no locks, fully picklable); the actual Node
    -- which owns a ResourceManager with a threading.Lock -- is built
    HERE, fresh, inside the child process, after the handoff. This is
    what makes node startup work identically on Linux, macOS and Windows.
    """
    node = Node(
        config.name, config.node_type, config.host, config.port,
        config.cpu_capacity, config.memory_capacity, config.bandwidth_capacity,
        scheduling_policy=config.scheduling_policy, packet_loss_rate=config.packet_loss_rate,
        election_id=config.election_id, services=config.services,
    )
    node.run_forever()


def start_node_process(config) -> OSProcess:
    """Launches a node described by `config` (a NodeConfig) as an independent OS process."""
    p = OSProcess(target=_node_entry, args=(config,), name=config.name, daemon=True)
    p.start()
    return p


def wait_until_up(host: str, port: int, timeout: float = 10.0) -> bool:
    """Polls a node with 'ping' until it responds or timeout elapses."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            reply = send_message(host, port, {"type": "ping"}, timeout=1.0)
            if reply.get("status") == "ok":
                return True
        except Exception:
            time.sleep(0.1)
    return False
