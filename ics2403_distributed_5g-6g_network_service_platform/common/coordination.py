"""
coordination.py
----------------
[MILESTONE 4 - Distributed Algorithms and Coordination]

Two coordination primitives, added as a new capability of the EXISTING
nodes built in Milestone 1 (not a separate system):

  1. LamportClock  - logical clocks for event ordering. Every node
     already gets one (see common/node.py); every message that flows
     through the platform's existing communication layer (M1) now
     carries a logical timestamp, so ANY two events anywhere in the
     system can be given a consistent total order.

  2. BullyElection - Garcia-Molina's Bully algorithm for leader
     election among the platform's nodes, using ONLY the request/reply
     transport already built in Milestone 1 (common/communication.py).
     Ranking is by `election_id` (see common/topology.py), which
     mirrors the resource-capacity hierarchy: Cloud/Core nodes are
     preferred coordinators over resource-poor Edge nodes.

This is deliberately a SIMPLIFIED, SYNCHRONOUS variant of Bully (the
orchestrator drives the run_election() call on behalf of an initiating
node) so that its behavior is easy to measure and reason about for
this milestone. Milestone 7 (Fault Tolerance) will trigger the exact
same algorithm automatically, in response to real detected node
failures, rather than an orchestrator-driven call.
"""

import time
from threading import Lock


class LamportClock:
    """A standard Lamport logical clock: one integer counter per node."""

    def __init__(self):
        self._time = 0
        self._lock = Lock()

    def tick(self) -> int:
        """Local/internal event (e.g. about to send a message): advance and return."""
        with self._lock:
            self._time += 1
            return self._time

    def update(self, received_ts: int) -> int:
        """On receiving a message stamped `received_ts`, apply Lamport's rule."""
        with self._lock:
            self._time = max(self._time, received_ts) + 1
            return self._time

    @property
    def value(self) -> int:
        with self._lock:
            return self._time


class BullyElection:
    """
    Drives one run of the Bully algorithm from the perspective of
    `self_name`. Peers with a HIGHER election_id are queried first;
    if none respond, `self_name` declares itself leader and notifies
    every peer. Message counts are tracked for complexity analysis.
    """

    def __init__(self, self_name: str, self_election_id: int, peers: dict, send_fn,
                 self_host: str = None, self_port: int = None):
        """
        peers: dict of name -> {"host":..., "port":..., "election_id":...}
               (does NOT include self_name)
        send_fn: callable(host, port, message_dict) -> reply_dict
                 (pass common.communication.send_message)
        self_host/self_port: this node's own address. When provided, the
        "coordinator" announcement is also sent to SELF (not just peers),
        so that the winning node updates its own `current_leader` field
        too -- otherwise a consistency check would find the leader is the
        only node that doesn't know it just won.
        """
        self.self_name = self_name
        self.self_election_id = self_election_id
        self.peers = peers
        self.send_fn = send_fn
        self.self_host = self_host
        self.self_port = self_port
        self.messages_sent = 0
        self.unreachable_peers = []

    def _higher_peers(self) -> dict:
        return {n: p for n, p in self.peers.items() if p["election_id"] > self.self_election_id}

    def run_election(self, timeout: float = 2.0) -> dict:
        """
        Executes one election round. Returns a dict describing the
        outcome (used directly for the milestone's experimental log):
            {"initiator", "declared_leader" or None, "messages_sent",
             "higher_peers_contacted", "responses_received", "elapsed_ms"}
        """
        t0 = time.time()
        higher = self._higher_peers()

        if not higher:
            # No one outranks self -> immediately become leader.
            self._announce_coordinator()
            elapsed_ms = (time.time() - t0) * 1000.0
            return {
                "initiator": self.self_name, "declared_leader": self.self_name,
                "messages_sent": self.messages_sent, "higher_peers_contacted": 0,
                "responses_received": 0, "elapsed_ms": round(elapsed_ms, 3),
            }

        responses = []
        for name, p in higher.items():
            try:
                reply = self.send_fn(p["host"], p["port"],
                                      {"type": "election", "from": self.self_name,
                                       "election_id": self.self_election_id}, timeout=timeout)
                self.messages_sent += 1
                if reply.get("status") == "ok":
                    responses.append(name)
            except Exception:
                self.unreachable_peers.append(name)  # treated as "did not respond"

        elapsed_ms = (time.time() - t0) * 1000.0

        if responses:
            # A higher-ranked node is alive and will take over the election
            # (in the real, fully-async Bully algorithm it starts its own
            # round immediately on replying "ok"; we return None here and
            # the orchestrator explicitly runs that node's round next, to
            # keep this synchronous variant observable step by step).
            return {
                "initiator": self.self_name, "declared_leader": None,
                "messages_sent": self.messages_sent, "higher_peers_contacted": len(higher),
                "responses_received": len(responses), "elapsed_ms": round(elapsed_ms, 3),
            }
        else:
            self._announce_coordinator()
            return {
                "initiator": self.self_name, "declared_leader": self.self_name,
                "messages_sent": self.messages_sent, "higher_peers_contacted": len(higher),
                "responses_received": 0, "elapsed_ms": round((time.time() - t0) * 1000.0, 3),
            }

    def _announce_coordinator(self):
        for name, p in self.peers.items():
            try:
                self.send_fn(p["host"], p["port"],
                             {"type": "coordinator", "leader": self.self_name}, timeout=2.0)
                self.messages_sent += 1
            except Exception:
                self.unreachable_peers.append(name)
        # Tell SELF too, so the winning node's own `current_leader` is consistent
        # with what it just told everyone else.
        if self.self_host is not None and self.self_port is not None:
            try:
                self.send_fn(self.self_host, self.self_port,
                             {"type": "coordinator", "leader": self.self_name}, timeout=2.0)
                self.messages_sent += 1
            except Exception:
                pass
