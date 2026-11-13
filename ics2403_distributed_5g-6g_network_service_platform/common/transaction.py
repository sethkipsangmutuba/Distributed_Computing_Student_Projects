"""
transaction.py
---------------
[MILESTONE 5 - Distributed Transactions]

Implements the Two-Phase Commit (2PC) protocol so that a single logical
operation spanning SEVERAL nodes (e.g. "establish a 5G session," which
needs a resource reservation at an Edge node AND at the Core node at the
same time) either succeeds everywhere or fails everywhere -- never
"half-succeeds" and leaves the platform in an inconsistent state.

Concept, in plain terms:
    PREPARE phase  - the coordinator asks every participant "COULD you
                      do this, if asked?" Each participant checks (but
                      does not yet apply) and votes COMMIT or ABORT.
    COMMIT/ABORT   - if EVERY participant voted COMMIT, the coordinator
                      tells everyone to actually apply the change. If
                      ANY participant voted ABORT (or didn't answer),
                      the coordinator tells everyone to discard it --
                      so nobody is left half-committed.
    COMPLETION     - the coordinator records the final outcome and
                      collects acknowledgements, which is what lets us
                      measure recovery behaviour when a participant is
                      unreachable.

This directly implements the "Prepare -> Commit -> Completion" model
named in the milestone brief, and gives the platform two of the four
ACID properties immediately (Atomicity, Consistency); Isolation is
provided by each node's existing ResourceManager lock (Milestone 1);
Durability is approximated by writing the transaction log to disk.
"""

import time
import itertools
import uuid

_tx_counter = itertools.count(1)


class TwoPhaseCommitCoordinator:
    """
    Runs ONE transaction (a resource reservation spanning several
    participant nodes) through the full 2PC protocol.

    participants: dict of node_name -> {"host":..., "port":..., "reservation": {...}}
                  where "reservation" is the cpu/memory/bandwidth this
                  transaction wants to reserve AT THAT PARTICIPANT.
    send_fn: callable(host, port, message, timeout) -> reply dict
             (pass common.communication.send_message)
    """

    def __init__(self, participants: dict, send_fn, prepare_timeout: float = 2.0):
        self.tx_id = f"tx-{next(_tx_counter)}-{uuid.uuid4().hex[:6]}"
        self.participants = participants
        self.send_fn = send_fn
        self.prepare_timeout = prepare_timeout
        self.log = []  # ordered list of (phase, detail) for the engineering log / report

    def _record(self, phase, detail):
        self.log.append({"t": round(time.time(), 6), "phase": phase, "detail": detail})

    def run(self) -> dict:
        t0 = time.time()

        # ---------------- PHASE 1: PREPARE ----------------
        votes = {}
        for name, p in self.participants.items():
            try:
                reply = self.send_fn(p["host"], p["port"], {
                    "type": "tx_prepare", "tx_id": self.tx_id, "reservation": p["reservation"],
                }, timeout=self.prepare_timeout)
                votes[name] = reply.get("vote", "abort")
            except Exception as e:
                votes[name] = "unreachable"
            self._record("prepare", {"participant": name, "vote": votes[name]})

        all_commit = all(v == "commit" for v in votes.values())

        # ---------------- DECISION ----------------
        decision = "commit" if all_commit else "abort"
        self._record("decision", {"decision": decision, "votes": votes})

        # ---------------- PHASE 2: COMMIT or ABORT, + COMPLETION ----------------
        acks = {}
        unreachable_on_completion = []
        for name, p in self.participants.items():
            msg_type = "tx_commit" if decision == "commit" else "tx_abort"
            # Recovery behaviour: retry a couple of times before giving up on
            # a participant during the completion phase (this is what lets
            # the system recover from a brief, transient unreachability
            # rather than a single dropped packet permanently corrupting
            # the transaction's outcome).
            ack = None
            for attempt in range(3):
                try:
                    reply = self.send_fn(p["host"], p["port"], {
                        "type": msg_type, "tx_id": self.tx_id,
                    }, timeout=self.prepare_timeout)
                    ack = reply.get("status")
                    break
                except Exception:
                    time.sleep(0.05 * (attempt + 1))  # brief backoff, then retry
            if ack is None:
                unreachable_on_completion.append(name)
                ack = "unreachable"
            acks[name] = ack
            self._record("completion", {"participant": name, "action": msg_type, "ack": ack})

        elapsed_ms = (time.time() - t0) * 1000.0

        return {
            "tx_id": self.tx_id,
            "decision": decision,
            "votes": votes,
            "acks": acks,
            "unreachable_on_completion": unreachable_on_completion,
            "fully_consistent": all(a in ("committed", "aborted") for a in acks.values()),
            "elapsed_ms": round(elapsed_ms, 3),
            "log": self.log,
        }
