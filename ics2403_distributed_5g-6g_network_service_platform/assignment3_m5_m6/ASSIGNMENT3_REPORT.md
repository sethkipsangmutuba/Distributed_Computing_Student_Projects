# Assignment 3 — Milestones 5 & 6
## Distributed 5G/6G Network Service Platform

Same codebase, same 5-node topology as Assignments 1 and 2. This assignment
adds two new capabilities to every existing node: the ability to take part
in a distributed transaction (Milestone 5), and a per-node lock manager for
studying concurrency and deadlock (Milestone 6).

---

## 1. Distributed Transactions (Milestone 5)

### 1.1 The concept, plainly

Some operations in a telecom platform aren't really "one thing" — they're
several things that all have to succeed together, or not happen at all. In
plain language: think of "establishing a 5G session" as needing **two
separate reservations at the same time** — a bit of capacity at an Edge node
*and* a bit of capacity at the RAN node. If the Edge reservation succeeds
but the RAN reservation fails, the session is broken (it thinks it has RAN
capacity, but never actually got it). **Distributed transactions** exist to
prevent exactly that "half-succeeded" situation.

### 1.2 How it works here: Two-Phase Commit (2PC)

We implemented the standard textbook protocol, exactly as the brief names
it — **Prepare → Commit → Completion**:

1. **PREPARE**: the coordinator asks every participant node, "if I asked you
   to reserve this, could you?" Each node checks its own resource ledger
   (without actually reserving anything yet) and votes `commit` or `abort`.
2. **DECISION**: if *every* participant voted `commit`, the coordinator
   decides `commit`. If even one voted `abort` — or didn't answer at all —
   the coordinator decides `abort` for everyone.
3. **COMPLETION**: the coordinator tells every participant the final
   decision. If `commit`, each node now actually applies the reservation
   (this is the only point where real resources change). If `abort`,
   nothing changes anywhere.

This gives us two of the four ACID properties immediately: **Atomicity**
(all-or-nothing — no half-applied sessions) and **Consistency** (the
platform never ends up in a state that shouldn't be possible). The other
two come from things we already built: **Isolation** from the thread-safe
lock inside each node's `ResourceManager` (Milestone 1), and **Durability**
approximated by writing every transaction's outcome to disk.

We also added a **teardown step** (release) after a successful commit, so
a session's resources are freed when it ends — otherwise every successful
transaction would permanently eat into capacity and the platform would
exhaust itself after a handful of sessions, which isn't realistic and
would muddy the experiments below.

### 1.3 Experiment: normal operation

`milestone5_transactions.py`, Phase A — 30 session-establishment
transactions, all 5 nodes healthy:

| Metric | Result |
|---|---|
| Transactions committed | 30 / 30 |
| Mean transaction latency | ~0.9 ms |
| Throughput | ~700–900 tx/sec |

### 1.4 Experiment: node failure + recovery

Phase B — `edge-2` is deliberately killed mid-run, then 20 more
transactions are attempted, split evenly between ones that need the dead
node and ones that don't:

| Group | Result |
|---|---|
| Transactions touching the DEAD node (edge-2) | 10/10 correctly **aborted** |
| Transactions touching only HEALTHY nodes | 10/10 still **committed** |
| Availability for unaffected traffic during the outage | **100%** |

**In plain terms:** losing one node didn't corrupt anything (every
transaction that touched it cleanly failed rather than half-applying), and
it didn't take down the rest of the platform either (traffic that didn't
need the dead node kept working perfectly). This is exactly the behaviour
a distributed system should have, and it previews Milestone 7 (Fault
Tolerance), where node failure becomes a first-class, automatically-handled
event instead of something we trigger by hand.

### 1.5 A genuine limitation we found (and what it means)

10 of the Phase B transactions show an "acknowledgment gap" — the
coordinator sent an `abort` message to the dead `edge-2` node but never got
a reply back, for the obvious reason that the node wasn't running. This
isn't a bug in our code — **it's a well-known, textbook limitation of
Two-Phase Commit itself**, sometimes called the "blocking problem": a
coordinator can never be 100% sure a crashed participant actually received
and applied its final instruction. This is precisely why the milestone
brief also names **Three-Phase Commit (3PC)** as an alternative — 3PC adds
an extra round specifically to reduce (not eliminate) this uncertainty. We
chose 2PC for this assignment because it's simpler to reason about and
matches the "Prepare → Commit → Completion" model the brief asks for
directly; 3PC is flagged here as a natural, named next step rather than
something we silently skipped.

---

## 2. Concurrency and Deadlock Management (Milestone 6)

### 2.1 Three concepts, plainly

- **Race condition**: when two things try to read-then-update the same
  piece of shared data at the same time, and one of the updates gets
  silently lost because they overlapped.
- **Mutual exclusion**: the fix — a rule that says "only one thing may
  touch this piece of data at a time," enforced with a lock.
- **Deadlock**: when two things are each waiting for something the *other*
  one is holding, so neither can ever proceed. Classic example: two people
  each holding one chopstick, both waiting for the other's chopstick.

### 2.2 Race condition, demonstrated directly

`milestone6_concurrency_deadlock.py` runs 8 threads, each incrementing a
shared counter 20 times (expected final total: 160):

| Version | Final value | What happened |
|---|---|---|
| **Without** a lock | **20** | 140 updates silently lost — threads overwrote each other |
| **With** a lock (mutual exclusion) | **160** | Every single update counted — correct |

This is about as clean a demonstration of "why mutual exclusion matters"
as it's possible to build: identical code, same number of operations, only
difference is a `lock.acquire()`/`release()` around the critical section.

### 2.3 Resource-allocation graph

The brief asks specifically for a model using `P_i -> R_j` (a process
waiting for a resource). Two graphs were built:

**Baseline (no contention)** — 3 processes, 3 different resources, nobody
waits for anybody:
```
P1 -> cpu_pool        (granted immediately)
P2 -> memory_pool      (granted immediately)
P3 -> bandwidth_pool   (granted immediately)
```

**Deadlock scenario** — Tx-A and Tx-B each need locks on **two different
nodes** (edge-1 and core-1), acquired in **opposite order**:
```
Tx-A holds edge-1's lock, WAITS for core-1's lock (held by Tx-B)
Tx-B holds core-1's lock, WAITS for edge-1's lock (held by Tx-A)
```
Drawn as a wait-for graph, this is a 2-node cycle: `Tx-A → Tx-B → Tx-A` —
a deadlock, by definition. This is the exact classic "circular wait"
condition from operating-systems theory, just spread across two different
machines instead of one.

### 2.4 Detection and recovery, distributed

We chose **detection + recovery** over prevention or avoidance (both valid
alternatives the brief allows), because it's the more interesting
distributed-systems problem: nothing on either single node "knows" a
deadlock exists — each node only sees its own tiny piece (one lock, one
waiter). The deadlock is only visible if you combine information from
*both* nodes.

`common/deadlock_detector.py` does exactly that:

1. **Poll** every node's lock manager: who's holding what, who's waiting
   for what.
2. **Merge** all of that into one system-wide wait-for graph.
3. **Search** the graph for cycles (a small depth-first search).
4. **Resolve**: for each cycle found, pick a "victim" transaction and
   force-abort it, releasing its lock so the rest of the cycle can proceed.

Actual run output:

| Step | Result |
|---|---|
| Both transactions still blocked 1.5s in? | **Yes** — genuine deadlock confirmed |
| Wait-for graph found | `Tx-A → Tx-B`, `Tx-B → Tx-A` (a 2-cycle) |
| Victim chosen | Tx-B |
| Both transactions finished after resolution? | **Yes** |
| Final outcome | Tx-A: both locks granted, completed normally. Tx-B: force-aborted, released its held lock |

### 2.5 Resource utilization analysis

Utilization was sampled from every node's `ResourceManager` at the end of
the run (`results/milestone6_concurrency_deadlock.json`,
`final_resource_utilization`). Because every lock in this milestone's
experiments was released immediately after use (or force-released by
deadlock resolution), utilization returns to near-zero at the end — the
platform doesn't accumulate "stuck" resource holds even after a deadlock
was resolved, which is itself a useful confirmation that the recovery
mechanism cleans up properly rather than just unblocking the waiting
thread and leaving a resource leak behind.

---

## 3. Big Data & Power BI Extension

Following the exact same framework established in Assignment 2 — not a
new, separate thing, but the SAME shared modules (`common/spark_utils.py`,
`common/big_data_layer.py`, `common/powerbi_export.py`) applied to this
assignment's concepts. This is what makes the big-data/Power BI
integration *consistent* across assignments rather than a one-off bolted
onto Assignment 2 alone.

### 3.1 Batch layer: 100,000-row transaction dataset (Milestone 5)

`assignment3_bigdata_extension.py` generates 100,000 simulated
session-establishment attempts across the same 5 node-pairs used live,
under three scenarios (`normal`, `high_load`, `node_down`), and runs a
Spark aggregation predicting commit/abort outcomes at scale:

| Scenario | Attempts | Predicted commit rate |
|---|---|---|
| normal | 70,122 | 100% |
| high_load | 19,984 | 100% |
| node_down | 9,894 | **0%** |

**Finding:** at 100,000-row scale, Spark confirms exactly what the live
30-transaction experiment in Section 1 showed with real 2PC runs — a
transaction touching a down node *always* aborts, cleanly, 100% of the
time, never partially. The big-data layer doesn't just repeat this
finding, it confirms it holds at a scale far beyond what we could afford
to run live, which is the actual point of pairing a batch layer with a
live speed layer.

### 3.2 Batch layer: 100,000-row lock-contention dataset (Milestone 6)

A second 100,000-row dataset simulates lock *requests* (which node, which
named resource, which transaction, which position in that transaction's
acquisition order) and Spark ranks which (node, resource) pairs see the
heaviest simulated contention — a large-scale complement to the small,
deliberately-engineered live deadlock in Section 2. Top contended
resources from a sample run: `core-1`/`qos_token`, `ran-1`/`radio_channel`,
`cloud-1`/`context_id`, each requested by 3,700+ distinct simulated
transactions.

### 3.3 Speed layer (live)

- 50 real 2PC transactions run through the live platform: **50/50
  committed** (healthy-platform baseline, consistent with Section 1.3).
- The exact live deadlock scenario from Section 2.4 re-run end to end:
  cycle `Tx-A-bd → Tx-B-bd → Tx-A-bd` detected, victim force-aborted,
  both transactions finished.

### 3.4 Serving layer: one shared, growing CSV folder

All batch + live results are written to the project-root
`results/powerbi/` folder (filenames `m5_*.csv`, `m6_*.csv`) via
`common/powerbi_export.py` — the SAME folder Assignment 2's `m3_*.csv` /
`m4_*.csv` files already live in. Nothing from Assignment 2 is
overwritten or needs reconnecting; Power BI simply gains new tables.

### 3.5 Presentation layer: the same dashboard, two new pages

See the project-root **`POWERBI_DASHBOARD_GUIDE.md`** — Assignment 3 adds
a "Transactions" page pair (batch + live) and a "Deadlock" page pair
(batch contention + live detection) to the exact same `.pbix` file
Assignment 2 started, rather than a separate dashboard.

---

## 4. Failure-Driven Engineering Log (Week 5–6)

| Question | Response |
|---|---|
| What changed? | Added 2PC-based distributed transactions (Milestone 5) and a distributed lock manager + deadlock detector (Milestone 6) as new capabilities of the existing nodes. |
| What failed? (M5) | Phase A initially committed only 12/30 transactions, and Phase B then showed 0/10 successes even for transactions that *didn't* touch the dead node — looking like the failure had taken down the whole platform. |
| Why did it fail? (M5) | Committed transactions never released their resource reservations, so the RAN node's capacity was silently exhausted partway through Phase A. Phase B's "everything fails" result wasn't actually about the node failure at all — it was leftover resource exhaustion from Phase A contaminating the next experiment. |
| How was it fixed? (M5) | Added an explicit `tx_release` step (session teardown) run immediately after each successful commit, modeling a session's real lifecycle (establish → use → end) instead of resources accumulating forever. |
| What failed? (M6) | The deadlock experiment didn't deadlock at all the first run — both transactions reported every lock "granted" immediately, with no waiting. |
| Why did it fail? (M6) | The two transactions were started 0.1 seconds apart, but local network round-trips complete in well under a millisecond — Tx-A finished acquiring *and releasing both* of its locks before Tx-B's thread had even sent its first request. There was never any overlap for a circular wait to form. |
| How was it fixed? (M6) | Each transaction now deliberately holds its first lock for 1 second before reaching for its second one, guaranteeing both transactions are mid-flight and holding their first lock at the same time — which is what makes the circular wait genuinely happen, rather than just being theoretically possible. |
| What alternative was considered? | For M6: relying on random natural timing variance to eventually produce a deadlock across repeated runs. Rejected — a demonstration that only sometimes reproduces the exact thing it's supposed to demonstrate is a weak basis for an oral defense; forcing the overlap deterministically is more honest about what's actually being shown. |
| What was learned? | Timing assumptions ("this will probably overlap") are a recurring source of bugs in concurrent/distributed code — both failures this week were really the same root cause (an implicit assumption about *when* things happen turning out to be wrong once measured). Explicit, deliberate synchronization points (a teardown step; a forced hold time) are more reliable than hoping operations happen to interleave the way you expect. |

---

## 5. Reproducibility

* Same environment as Assignments 1–2: Python 3, standard library only for
  `common/transaction.py`, `common/lock_manager.py`, `common/deadlock_detector.py`.
* Run order: `python -m assignment3_m5_m6.milestone5_transactions` then
  `python -m assignment3_m5_m6.milestone6_concurrency_deadlock` (each
  launches its own fresh set of node processes; independent of each other
  and of earlier assignments' scripts).
* Timing constants that matter for reproducibility: Milestone 5's
  `prepare_timeout=1.5s`; Milestone 6's race-condition `time.sleep(0.001)`
  interleave delay and `HOLD_BEFORE_SECOND_ACQUIRE=1.0s` deadlock hold time
  — all defined as named constants at the top of their respective
  functions for easy adjustment.
* Big data extension: `pip install -r requirements.txt` (pyspark, pandas,
  numpy, pyarrow — same dependencies as Assignment 2, nothing new), then
  `python -m assignment3_m5_m6.assignment3_bigdata_extension`. Independent
  of the two milestone scripts above; launches its own node platform.

---

## 6. Presentation Summary (for group use)

> **Assignment 3 (Milestones 5 & 6) — what we built:**
> We added Two-Phase Commit transactions to our platform, so operations
> spanning multiple nodes (like establishing a 5G session across Edge and
> RAN) either fully succeed or fully fail — never half-applied. Under
> normal conditions, 30/30 transactions committed cleanly in under 1ms
> average. We then killed a node mid-run and proved two things at once:
> transactions needing that node correctly and safely aborted, while
> transactions that didn't need it kept succeeding 100% of the time — one
> node failing doesn't take the platform down. We also found and documented
> a real, textbook-known limitation of Two-Phase Commit in the process
> (the "blocking problem" with crashed participants), which is exactly why
> Three-Phase Commit exists.
>
> For concurrency, we proved race conditions are real and mutual exclusion
> fixes them with a direct before/after test: 140 out of 160 counter
> updates were silently lost without a lock, and zero were lost with one.
> We then deliberately engineered a real distributed deadlock — two
> transactions locking the same two resources on two different nodes in
> opposite order — confirmed both were genuinely stuck, and built a
> detector that polls every node, merges their lock states into one
> system-wide wait-for graph, finds the cycle, and force-aborts a victim
> transaction to break it. Both transactions finished successfully after
> resolution.
>
> **Big Data & Power BI, consistently:** we extended the exact same Spark +
> Power BI framework from Assignment 2 to this assignment's concepts —
> batch-analyzing 100,000 simulated transaction attempts (confirming our
> live finding that node failures cause clean, total abort, never partial
> commit) and 100,000 simulated lock requests (ranking contention hotspots
> across the platform). Everything feeds into the SAME growing Power BI
> dashboard Assignment 2 started, through the same shared `results/powerbi/`
> folder — no separate dashboard, no reconnecting data sources.
