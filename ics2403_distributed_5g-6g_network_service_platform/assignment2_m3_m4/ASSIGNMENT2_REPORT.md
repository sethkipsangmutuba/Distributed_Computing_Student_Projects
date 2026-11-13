# Assignment 2 — Milestones 3 & 4
## Distributed 5G/6G Network Service Platform

This assignment does not rebuild the system — it extends the exact same
`common/` codebase and 5-node topology from Assignment 1 with architectural
structure (M3) and coordination behaviour (M4).

---

## 1. Distributed Architecture (Milestone 3)

### 1.1 Architectural style chosen

Of the four styles the brief asks to evaluate:

| Style | Verdict for this platform |
|---|---|
| Client–server | Too flat — doesn't capture the Edge/RAN/Core/Cloud hierarchy |
| Multi-tier | Partially true (Edge→RAN→Core→Cloud is tiered) but doesn't capture *what* each tier does |
| **Service-oriented / microservice** | **Chosen.** Each node type hosts a distinct, independently addressable set of services |

We combine the two: **a multi-tier deployment topology, with a
microservice/service-oriented decomposition inside each tier.** This is
implemented directly, not just described — every node now advertises its
services over the wire (`service_info` message, `common/topology.py::SERVICES_BY_TYPE`):

| Node type | Services hosted |
|---|---|
| EDGE | SessionHandling, LocalQoSScheduling |
| RAN | RadioResourceControl, LocalQoSScheduling |
| CORE | ResourceOrchestration, MobilityManagement, **Coordination** |
| CLOUD | Analytics, LongTermStorage, PolicyManagement |

(The `Coordination` service on Core foreshadows Milestone 4's leader
election, and `election_id` rankings in `topology.py` mean Core/Cloud are
preferred coordinators — Core hosts it explicitly, Cloud ends up elected
leader in our M4 runs since it ranks highest.)

### 1.2 Deployment architecture

```
        Users / Devices
              │
      ┌───────┴────────┐
   edge-1            edge-2      Tier: EDGE   — SessionHandling, LocalQoSScheduling
      │                 │
      └───────┬─────────┘
             ran-1                Tier: RAN    — RadioResourceControl, LocalQoSScheduling
               │
             core-1                Tier: CORE   — ResourceOrchestration, MobilityManagement, Coordination
               │
             cloud-1                Tier: CLOUD  — Analytics, LongTermStorage, PolicyManagement
```

Unchanged from Milestone 1 — the point of Milestone 3 is that this topology
*is already* a valid distributed architecture; what's new is naming it
precisely and testing its scaling/trade-off properties.

### 1.3 Scalability analysis

Same 90-task workload, same concurrency (6), only the number of Edge nodes
handling it changes (`assignment2_m3_m4/results/milestone3_architecture.json`):

| Edge nodes | Throughput (tasks/s) | Mean latency | Packet loss / rejection |
|---|---|---|---|
| 1 | 71.4 | 24.3 ms | **93.3%** |
| 2 | 236.8 | 24.4 ms | 32.2% |
| 4 | 175.7 | 31.4 ms | **7.8%** |

**Finding:** loss/rejection drops sharply and monotonically as edge capacity
scales out (93% → 32% → 7.8%) — this is the expected, desirable scalability
behaviour: more nodes absorb the same offered load with far fewer resource
rejections. Throughput itself is *not* monotonic (peaks at 2, dips slightly
at 4) because concurrency was held fixed at 6 clients while work was spread
across more destinations — a reminder that throughput and reliability are
different axes, and that client-side concurrency should scale alongside
server-side capacity to fully exploit it. This is flagged as a parameter to
vary properly in the Final Capstone Experiment (Section 5 of the brief).

### 1.4 Edge vs Core trade-off analysis

Same workload, sent to *only* the two edge nodes vs *only* `core-1`:

| Placement | Throughput | Latency | Packet loss / rejection |
|---|---|---|---|
| Edge-only | 237.0 tasks/s | 24.6 ms | 33.3% |
| Core-only | 201.2 tasks/s | 26.9 ms | **11.1%** |

**Interpretation:** two weak edge nodes together out-throughput one stronger
core node, but the single core node is more reliable per task (lower
rejection) because its individually larger capacity absorbs bursts better
even without horizontal scale-out. Neither placement is "correct" in
isolation — the right answer depends on the QoS class (URLLC wants edge
proximity; eMBB wants core capacity). This directly motivates the
**edge-aware placement** used as the baseline in Assignment 1 and the
**proposed improvement** to be formalized in Milestone 5.

---

## 2. Distributed Algorithms and Coordination (Milestone 4)

### 2.1 Leader election — Bully algorithm

Implemented in `common/coordination.py::BullyElection`, using nothing but
the Milestone-1 TCP/JSON transport. Ranking (`election_id`) mirrors the
resource-capacity hierarchy, so the platform prefers its most capable node
(Cloud) as coordinator — realistic for a telecom platform where the
coordination function should sit where there's spare compute, not at the
resource-constrained edge.

Three elections were run from different starting points
(`assignment2_m3_m4/results/milestone4_coordination.json`):

| Initiator | Rounds | Messages | Time | Elected leader | Consistent across all 5 nodes? |
|---|---|---|---|---|---|
| edge-1 (lowest rank) | 5 | 15 | 2.18 ms | cloud-1 | ✅ Yes |
| ran-1 (mid rank) | 3 | 8 | 1.10 ms | cloud-1 | ✅ Yes |
| cloud-1 (highest rank) | 1 | 5 | 0.72 ms | cloud-1 | ✅ Yes |

**Message complexity:** message count scales with how many nodes outrank
the initiator — edge-1 (outranked by all 4 others) took the most rounds and
messages; cloud-1 (outranked by none) won immediately with a single
broadcast round. This matches Bully's known worst case of O(n²) messages
across a cascade of initiators, and its best case of O(n) for a single
broadcast.

**Consistency:** after every run, all 5 nodes were independently queried
(`leader_query`) and unanimously agreed on the same leader — the initial
run surfaced and then fixed a real bug where the winning node never told
*itself* it had won (see the engineering log below).

**Synchronization delay:** sub-3ms for all three scenarios, since all
communication is local-network TCP; a genuinely geo-distributed deployment
would show this delay dominated by round-trip network latency instead.

### 2.2 Logical clocks — event ordering

Every message handled anywhere in the platform now updates a **Lamport
clock** per node (`common/node.py`, universal hook in `_handle_message`).
Three concurrent client "processes" sent interleaved pings to different
nodes; sorting the resulting events by their Lamport timestamp reconstructs
one consistent total order even though the events originated from separate
threads hitting separate nodes with no shared clock:

```
lamport=  8  client-B -> edge-1
lamport=  9  client-C -> edge-1
lamport= 10  client-A -> edge-1
lamport= 13  client-A -> core-1
lamport= 14  client-C -> core-1
lamport= 15  client-B -> core-1
```

This demonstrates the core guarantee Lamport clocks provide: a **causal,
consistent ordering** of distributed events without a shared physical clock
— exactly what later milestones (M5 transaction ordering, M9 consistency)
will depend on.

---

## 3. Big Data & Power BI Extension

This is an additional layer built on top of Milestones 3 and 4 — it does not
replace or reduce anything above; `milestone3_architecture.py` and
`milestone4_coordination.py` still run standalone and satisfy the brief's
objectives exactly as described in Sections 1 and 2. This section documents
the extra requirement: a 100,000-row workload dataset, a big data technology,
and a Power BI dashboard.

### 3.1 Chosen big data technology: Apache Spark (PySpark, local mode)

**Why Spark over alternatives (e.g. Kafka, Hadoop/HDFS):** Spark runs as a
single embedded process — `pip install pyspark` plus a JDK is the entire
setup, with no separate broker, cluster manager, or coordination service
(e.g. Zookeeper) to install and keep alive. It is nonetheless a genuine,
industry-standard distributed batch-processing engine: even in local mode,
it explicitly partitions the dataset (8 partitions in our runs) and executes
aggregations across those partitions using the same DataFrame execution
model it uses on a real cluster. This made it the practical choice for a
project that needs to run identically on group members' personal laptops.

### 3.2 Architecture: a Lambda Architecture extension

Rather than bolting big data on as an unrelated add-on, it was integrated as
a formal architectural pattern — a **Lambda Architecture** — which directly
extends Milestone 3's architecture analysis rather than sitting outside it:

```
   [BATCH LAYER]      Apache Spark processes the full 100,000-row
                       historical telecom workload dataset
                              │
   [SPEED LAYER]      The SAME live Edge/RAN/Core/Cloud node platform
                       (Milestones 1-4, completely unchanged) processes
                       a real-time, stratified 600-task sample
                              │
   [SERVING LAYER]    Batch + Speed + M3 scalability + M4 coordination
                       results are merged into Power BI-ready CSVs
                              │
   [PRESENTATION]     Power BI Desktop dashboard
```

The Speed layer is not a new system — it is literally the same
`common/node.py` platform, launched the same way, so every Milestone 1-4
objective (process/resource model, communication, service decomposition,
scalability, leader election, logical clocks) is exercised identically to
before, just alongside a new batch layer rather than instead of anything.

### 3.3 Batch layer results (100,000-row dataset)

`common/big_data_layer.py::run_batch_layer()` generates the dataset
(vectorized with numpy — a plain 100,000-iteration Python loop was
considered and rejected as the first obvious bottleneck at this scale),
loads it into Spark, repartitions it across 8 partitions, and runs two
distributed aggregations:

**By service class** (project-root `results/bigdata/batch_summary_by_service_class.csv`):

| Service class | Task count | Avg CPU | Avg memory (MB) | Total bandwidth demand (Mbps) |
|---|---|---|---|---|
| mMTC | 49,720 | 1.00 | 19.96 | 99,416.6 |
| eMBB | 30,023 | 3.00 | 320.34 | 898,471.7 |
| URLLC | 20,257 | 1.50 | 79.74 | 121,625.7 |

**By simulated node placement** (project-root `results/bigdata/batch_summary_by_node.csv`),
using the exact same edge-first placement rule as the live platform, applied
via Spark to all 100,000 rows:

| Node | Simulated task count | Total CPU demand |
|---|---|---|
| edge-2 | 35,003 | 39,958.1 |
| edge-1 | 34,974 | 40,134.4 |
| ran-1 | 30,023 | 89,952.7 |

**Finding:** even though `ran-1` receives fewer tasks than either edge node,
its total CPU demand is roughly double — because it absorbs 100% of the
resource-heavy eMBB traffic. This is the same "ran-1 is the bottleneck"
finding from Milestone 2/3's small-scale experiments, now confirmed at
full 100,000-row scale using genuine distributed batch processing rather
than a 90-120 task sample — the big data layer corroborates, not just
decorates, the earlier architectural analysis.

### 3.4 Speed layer results (live 600-task stratified sample)

A proportional sample (same service-class mix as the full dataset) was
replayed through the real live node platform:

| Metric | Result |
|---|---|
| Throughput | ~783 tasks/sec |
| Mean latency | ~11.2 ms |
| Packet loss / rejection | ~46% |

The rejection rate is higher here than in Milestone 2/3's smaller runs
because 600 concurrent-ish tasks against the same fixed edge/RAN capacity
is a heavier offered load — consistent with, not contradicting, the
capacity-bottleneck finding already established. It reinforces the case for
the edge-aware placement improvement planned for Milestone 5.

### 3.5 Power BI dashboard

All batch-layer, speed-layer, scalability (M3), and election (M4) results
are exported as clean CSVs to the project-root `results/powerbi/` folder
(filenames prefixed `m3_`/`m4_`), NOT an assignment-local folder — this is
deliberate: every assignment from here on writes into this SAME shared
folder (via `common/powerbi_export.py`), so ONE Power BI dashboard grows
across the whole project rather than a new, disconnected dashboard being
built per assignment. See the project-root **`POWERBI_DASHBOARD_GUIDE.md`**
for the exact steps (a `.pbix` file cannot be generated by a script — it's
a binary format the Power BI Desktop application creates itself). This
assignment contributes 5 pages: Scalability, Coordination, Architecture,
Batch Layer (100K rows), and Speed Layer (live sample); Assignment 3 adds
2 more to the same file.

### 3.6 Reproducibility (extension)

* Additional dependencies: `pyspark`, `pandas`, `numpy`, `pyarrow` (all
  `pip install ... --break-system-packages` on Linux, or plain `pip install`
  in a normal Windows Python environment), plus a JDK (Java 11/17/21 — Spark
  4.x requires a JDK; OpenJDK is free and works).
* Run: `python -m assignment2_m3_m4.assignment2_bigdata_extension` (run
  `milestone3_architecture.py` and `milestone4_coordination.py` first at
  least once, so their JSON results exist for the CSV re-export step).
* Dataset generation seed is fixed (`seed=2026`); the live-sample seed is
  fixed (`seed=99`) for reproducibility of which rows are replayed live.

---

## 4. Failure-Driven Engineering Log (Week 3–4)

| Question | Response |
|---|---|
| What changed? | Added service decomposition metadata to every node (M3), formalized the deployment architecture, ran scalability + edge-vs-core experiments, and implemented Bully leader election + Lamport logical clocks as new capabilities of the existing nodes (M4). |
| What failed? | First leader-election run reported the winning node (`cloud-1`) as inconsistent with the rest of the platform — every *other* node correctly recorded `cloud-1` as leader, but `cloud-1` itself still showed `current_leader: None`. |
| Why did it fail? | `BullyElection._announce_coordinator()` broadcast the "coordinator" message to every *peer* but never to itself — the winning node has no peer entry for its own address, so it never received (and thus never processed) its own win announcement. |
| How was it fixed? | `BullyElection` now optionally takes the initiator's own host/port and sends itself the same "coordinator" message it sends everyone else, so the winner's local state updates identically to every other node's. Verified with a consistency check across all 5 nodes after each of 3 election runs. |
| What alternative was considered? | Special-casing "if declared_leader == self_name, set current_leader locally without a network round-trip." Rejected in favour of sending a real (loopback) message to itself, since that exercises the exact same code path every other node uses and keeps the node's internal state changes reachable *only* through its message handler — simpler to reason about and consistent with how Milestone 9 (transparency) will later want every state change to look identical regardless of origin. |
| What was learned? | "Broadcast to all peers" is a common source of off-by-one distributed bugs — it's easy to define "peers" as "everyone but me" and then forget that "me" also needs the update. Consistency checks that poll *every* node after a coordination event (not just spot-check a few) are what caught this. |
| What changed? (extension) | Added the big data batch layer (Spark) and Power BI export/serving layer. |
| What failed? (extension) | (1) Spark's `createDataFrame(pandas_df)` initially processed the 100,000-row dataset as a single partition, undermining the "distributed batch processing" claim. (2) The stratified live sample used `groupby().apply()`, which silently dropped the `service_class` column under pandas 3.x's changed default behaviour, crashing the speed layer with a `KeyError`. |
| Why did it fail? | (1) Spark's local mode defaults partition count to available cores, and this development environment only exposed 1 core to the process. (2) pandas 3.x no longer passes the grouping column into `apply()` by default (`include_groups` behaviour changed), which earlier pandas versions did. |
| How was it fixed? | (1) Explicitly `.repartition(8)` after loading the DataFrame into Spark, so partitioning is deterministic and visible regardless of the host machine's core count. (2) Replaced `groupby().apply()` with an explicit per-class loop and `pd.concat`, which is robust across pandas versions and easier to read besides. |
| What alternative was considered? | For the partitioning issue: relying on `local[*]` to auto-detect cores on the group's own (likely multi-core) machines instead of forcing 8 partitions. Rejected — the result should be reproducible and demonstrably parallel regardless of which machine runs it. |
| What was learned? | Local-mode "distributed" processing needs explicit partitioning to be a trustworthy demonstration, not just an implicit consequence of hardware. Library version drift (pandas 2.x vs 3.x) is a real reproducibility risk worth pinning or defending against directly in code, not just noting in a requirements file. |

---

## 5. Reproducibility

* Same environment as Assignment 1: Python 3, standard library only.
* Topology, election ranks, and service assignments are defined in
  `common/topology.py`; workload seed is fixed (`seed=11` for M3, `seed`
  inherited from `generate_workload` defaults for M4's ping bursts).
* Run order: `python -m assignment2_m3_m4.milestone3_architecture` then
  `python -m assignment2_m3_m4.milestone4_coordination` (each launches its
  own fresh set of node processes; they don't depend on each other or on
  Assignment 1's scripts having been run first).

---

## 6. Presentation Summary (for group use)

> **Assignment 2 (Milestones 3 & 4) — what we built:**
> We formalized our platform's architecture as a layered, service-oriented
> system: Edge and RAN nodes handle session/radio-facing services, Core adds
> orchestration and coordination, Cloud adds analytics and policy. We proved
> this scales — spreading the same workload across 1, 2, then 4 edge nodes
> cut task rejection from 93% down to 8% — and quantified the edge-vs-core
> trade-off (edge wins on throughput, core wins on reliability), which sets
> up our "proposed improvement" for later milestones. We then added real
> distributed coordination on top of the same nodes: a Bully leader-election
> algorithm that always converges on our highest-capacity node (Cloud) with
> full consistency across all 5 nodes, and Lamport logical clocks that let
> us reconstruct one consistent event order from three concurrent client
> processes hitting different nodes with no shared clock.
>
> **Extension — Big Data & Power BI:** We integrated Apache Spark as a batch
> processing layer that ingests and analyzes a 100,000-row synthetic telecom
> workload dataset, following a Lambda Architecture: Spark handles the
> large-scale historical analysis while our existing live node platform
> still handles real-time task execution, unchanged. Spark's distributed
> aggregation (across 8 partitions) confirmed at full scale what our
> smaller experiments already suggested — `ran-1` absorbs disproportionate
> load because it hosts all eMBB traffic. All batch, live, scalability and
> coordination results are exported to CSV and wired into a 5-page Power BI
> dashboard (see `POWERBI_DASHBOARD_GUIDE.md`) for the group's presentation.
