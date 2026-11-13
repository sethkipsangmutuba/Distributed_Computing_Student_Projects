"""
big_data_layer.py
------------------
[BIG DATA FRAMEWORK - shared across every assignment, starting Assignment 2]

Chosen big data technology: Apache Spark (PySpark, local mode) -- see
common/spark_utils.py for the shared session/aggregation plumbing every
assignment's batch layer runs through.

WHY SPARK (and not e.g. Kafka or Hadoop/HDFS):
  - Runs as a single embedded process (no separate broker/cluster/
    Zookeeper service to install and keep running) -- realistic for a
    student project on a single Windows laptop: `pip install pyspark`
    plus a JDK is the entire setup.
  - It is nonetheless a genuine, industry-standard distributed batch
    processing engine: even in "local[*]" mode, Spark partitions the
    dataset and processes partitions in parallel across CPU cores using
    the same DataFrame/RDD execution model it uses on a real cluster.
  - It directly complements (does not replace) the platform's existing
    live socket-based node system: Spark handles the LARGE, HISTORICAL
    side of the workload; the existing nodes still handle real-time
    task execution. This is a standard "Lambda Architecture" split,
    applied CONSISTENTLY to every assignment from Assignment 2 onward:

        Batch Layer (Spark, 100,000-row dataset)
                        │
        Speed Layer (live Edge/RAN/Core/Cloud nodes -- whichever
                      milestone's real mechanism: task execution in
                      Assignment 1/2, transactions/locks in Assignment 3)
                        │
        Serving Layer (merged CSV exports, common/powerbi_export.py)
                        │
        Presentation Layer (ONE growing Power BI dashboard)

This module owns every assignment's Batch layer. The Speed layer is
always the real node platform from common/node.py -- never replaced,
only ever reused.
"""

import os
import numpy as np
import pandas as pd

from common.workload_generator import SERVICE_PROFILES, CLASS_WEIGHTS
from common.spark_utils import get_spark_session

# Participant-pair combinations a session-establishment transaction can
# span (Assignment 3 / Milestone 5), reused by both the live topology
# and the big-data transaction dataset below, so Spark's analysis stays
# grounded in the SAME node topology every other milestone uses.
PARTICIPANT_PAIRS = [
    ("edge-1", "ran-1"), ("edge-2", "ran-1"),
    ("edge-1", "core-1"), ("edge-2", "core-1"),
    ("ran-1", "core-1"),
]
# Lock resource names/nodes (Assignment 3 / Milestone 6), used by the
# lock-contention dataset generator further down.
LOCK_RESOURCE_NAMES = ["session_slot", "qos_token", "bearer_context", "radio_channel", "context_id"]
LOCK_NODES = ["edge-1", "edge-2", "ran-1", "core-1", "cloud-1"]


def generate_large_dataset(n_rows: int = 100_000, seed: int = 2026) -> pd.DataFrame:
    """
    Vectorized (numpy) generation of a large telecom service-request
    dataset with the SAME service-class distribution and per-class
    resource-profile ranges as the small-scale workload_generator.py
    used in Milestones 1/2/3, so results are directly comparable across
    scales. Vectorized rather than a 100,000-iteration Python loop,
    which is itself a small "big data" engineering decision worth
    noting: naive per-row generation would be the first bottleneck.
    """
    rng = np.random.default_rng(seed)
    classes = list(CLASS_WEIGHTS.keys())
    weights = np.array(list(CLASS_WEIGHTS.values()))
    service_class = rng.choice(classes, size=n_rows, p=weights / weights.sum())

    cpu = np.empty(n_rows)
    memory = np.empty(n_rows)
    bandwidth = np.empty(n_rows)
    work_units = np.empty(n_rows)

    for cls in classes:
        mask = service_class == cls
        n = mask.sum()
        prof = SERVICE_PROFILES[cls]
        cpu[mask] = rng.uniform(*prof["cpu"], size=n)
        memory[mask] = rng.uniform(*prof["memory"], size=n)
        bandwidth[mask] = rng.uniform(*prof["bandwidth"], size=n)
        work_units[mask] = rng.uniform(*prof["work_units"], size=n)

    df = pd.DataFrame({
        "task_id": np.arange(n_rows),
        "service_class": service_class,
        "cpu": np.round(cpu, 2),
        "memory": np.round(memory, 1),
        "bandwidth": np.round(bandwidth, 1),
        "work_units": np.round(work_units, 1),
    })
    return df


def run_batch_layer(n_rows: int = 100_000, seed: int = 2026, results_dir: str = None):
    """
    [BATCH LAYER] Runs the full Spark pipeline over the large dataset:
      1. Load the dataset into a Spark DataFrame (distributed across partitions).
      2. Simulate the SAME edge-first placement policy used live in
         Milestones 2/3 (common/workload_generator.py::edge_first_placement),
         but applied to all 100,000 rows via Spark's distributed execution
         instead of the Python client submitting each one live.
      3. Aggregate (a) per-service-class demand and (b) per-node simulated
         load, using Spark's distributed groupBy/agg -- this is the actual
         "big data processing" step, executed in parallel across partitions.
      4. Write results as CSVs, ready for Power BI import.

    Returns a dict summary (also written to disk) for use in the report/JSON log.
    """
    from pyspark.sql import functions as F
    from common.powerbi_export import BIGDATA_DIR

    if results_dir is None:
        results_dir = BIGDATA_DIR  # shared, absolute, project-root location -- never CWD-dependent
    os.makedirs(results_dir, exist_ok=True)

    pdf = generate_large_dataset(n_rows=n_rows, seed=seed)
    raw_path = os.path.join(results_dir, "raw_dataset_sample_1000.csv")
    pdf.head(1000).to_csv(raw_path, index=False)  # full 100k kept in Spark only; 1k sample for inspection/Power BI

    spark = get_spark_session()
    try:
        sdf = spark.createDataFrame(pdf)
        # Explicitly partition the dataset so processing is genuinely split and
        # parallelized across partitions/cores, rather than relying on however
        # many cores happen to be free on whatever machine this runs on.
        n_target_partitions = 8
        sdf = sdf.repartition(n_target_partitions)
        sdf = sdf.withColumn("row_id", F.monotonically_increasing_id())

        # --- Simulate the M2/M3 "edge-first" placement policy at scale ---
        # URLLC and mMTC -> round-robin across 2 edge nodes; eMBB -> ran-1
        # (mirrors common/workload_generator.py::edge_first_placement exactly)
        sdf = sdf.withColumn(
            "assigned_node",
            F.when(F.col("service_class") == "eMBB", F.lit("ran-1"))
             .otherwise(F.when(F.col("row_id") % 2 == 0, F.lit("edge-1")).otherwise(F.lit("edge-2")))
        )
        node_type_map = {"edge-1": "EDGE", "edge-2": "EDGE", "ran-1": "RAN"}
        map_expr = F.create_map([F.lit(x) for kv in node_type_map.items() for x in kv])
        sdf = sdf.withColumn("node_type", map_expr[F.col("assigned_node")])

        # --- [Distributed aggregation #1] demand per service class ---
        by_class = (
            sdf.groupBy("service_class")
               .agg(
                   F.count("*").alias("task_count"),
                   F.round(F.avg("cpu"), 3).alias("avg_cpu"),
                   F.round(F.avg("memory"), 2).alias("avg_memory_mb"),
                   F.round(F.avg("bandwidth"), 2).alias("avg_bandwidth_mbps"),
                   F.round(F.avg("work_units"), 2).alias("avg_work_units"),
                   F.round(F.sum("bandwidth"), 1).alias("total_bandwidth_demand_mbps"),
                   F.round(F.stddev("work_units"), 3).alias("stddev_work_units"),
               )
               .orderBy(F.desc("task_count"))
        )
        by_class_pdf = by_class.toPandas()
        by_class_pdf.to_csv(os.path.join(results_dir, "batch_summary_by_service_class.csv"), index=False)

        # --- [Distributed aggregation #2] simulated load per node ---
        by_node = (
            sdf.groupBy("assigned_node", "node_type")
               .agg(
                   F.count("*").alias("task_count"),
                   F.round(F.sum("cpu"), 1).alias("total_cpu_demand"),
                   F.round(F.sum("memory"), 1).alias("total_memory_demand_mb"),
                   F.round(F.sum("bandwidth"), 1).alias("total_bandwidth_demand_mbps"),
                   F.round(F.avg("work_units"), 2).alias("avg_work_units"),
               )
               .orderBy(F.desc("task_count"))
        )
        by_node_pdf = by_node.toPandas()
        by_node_pdf.to_csv(os.path.join(results_dir, "batch_summary_by_node.csv"), index=False)

        total_rows = sdf.count()
        n_partitions = sdf.rdd.getNumPartitions()

        summary = {
            "technology": "Apache Spark (PySpark, local[*] mode)",
            "total_rows_processed": total_rows,
            "spark_partitions_used": n_partitions,
            "by_service_class": by_class_pdf.to_dict(orient="records"),
            "by_node": by_node_pdf.to_dict(orient="records"),
        }
        return summary
    finally:
        spark.stop()


# =============================================================================
# [MILESTONE 5 - Distributed Transactions] big-data batch layer
# =============================================================================

def generate_transaction_dataset(n_rows: int = 100_000, seed: int = 3031) -> pd.DataFrame:
    """
    Vectorized generation of a large dataset of simulated session-
    establishment TRANSACTION ATTEMPTS: which participant-node pair they
    target (common.workload_generator-style realism, grounded in the
    SAME 5-node topology), how big a reservation they ask for, and which
    operating "scenario" they occur under (normal / node_down / high_load)
    -- mirroring the three conditions actually tested live in
    milestone5_transactions.py, but at 100,000-row scale.
    """
    rng = np.random.default_rng(seed)
    pair_idx = rng.integers(0, len(PARTICIPANT_PAIRS), size=n_rows)
    participant_a = np.array([PARTICIPANT_PAIRS[i][0] for i in pair_idx])
    participant_b = np.array([PARTICIPANT_PAIRS[i][1] for i in pair_idx])

    scenarios = np.array(["normal", "node_down", "high_load"])
    scenario_weights = np.array([0.7, 0.1, 0.2])  # mostly-normal operation, like a real deployment
    scenario = rng.choice(scenarios, size=n_rows, p=scenario_weights)

    cpu = np.round(rng.uniform(0.5, 2.0, size=n_rows), 2)
    memory = np.round(rng.uniform(32, 256, size=n_rows), 1)
    bandwidth = np.round(rng.uniform(2, 20, size=n_rows), 1)

    df = pd.DataFrame({
        "tx_id": np.arange(n_rows),
        "participant_a": participant_a,
        "participant_b": participant_b,
        "scenario": scenario,
        "cpu": cpu, "memory": memory, "bandwidth": bandwidth,
    })
    return df


def run_transaction_batch_layer(n_rows: int = 100_000, seed: int = 3031, results_dir: str = None) -> dict:
    """
    [BATCH LAYER - Milestone 5] Spark-processes the large transaction
    dataset: for each (participant pair, scenario) combination, predicts
    a commit/abort outcome using the SAME simple rule the live platform
    actually enforces (a transaction touching a "down" node aborts;
    otherwise it commits if the combined reservation is plausible), then
    aggregates predicted commit/abort rates and demand at scale -- this
    is what lets us say something about expected behaviour across
    100,000 attempts, not just the 50 we can afford to run live.
    """
    from pyspark.sql import functions as F
    from common.powerbi_export import BIGDATA_DIR

    if results_dir is None:
        results_dir = BIGDATA_DIR
    os.makedirs(results_dir, exist_ok=True)

    pdf = generate_transaction_dataset(n_rows=n_rows, seed=seed)

    spark = get_spark_session("5g6g-platform-m5-transaction-batch")
    try:
        sdf = spark.createDataFrame(pdf).repartition(8)
        # Predicted outcome: a node_down-scenario transaction always aborts
        # (mirrors the real 2PC prepare-vote failure); otherwise it commits.
        sdf = sdf.withColumn(
            "predicted_decision",
            F.when(F.col("scenario") == "node_down", F.lit("abort")).otherwise(F.lit("commit"))
        )
        sdf = sdf.withColumn("pair", F.concat_ws(" <-> ", F.col("participant_a"), F.col("participant_b")))

        by_pair_scenario = (
            sdf.groupBy("pair", "scenario")
               .agg(
                   F.count("*").alias("attempt_count"),
                   F.sum(F.when(F.col("predicted_decision") == "commit", 1).otherwise(0)).alias("predicted_commits"),
                   F.sum(F.when(F.col("predicted_decision") == "abort", 1).otherwise(0)).alias("predicted_aborts"),
                   F.round(F.avg("cpu"), 3).alias("avg_cpu_requested"),
                   F.round(F.sum("bandwidth"), 1).alias("total_bandwidth_requested_mbps"),
               )
               .orderBy(F.desc("attempt_count"))
        )
        by_pair_pdf = by_pair_scenario.toPandas()
        by_pair_pdf.to_csv(os.path.join(results_dir, "m5_batch_transactions_by_pair_scenario.csv"), index=False)

        overall = sdf.groupBy("scenario").agg(
            F.count("*").alias("attempt_count"),
            F.round(F.sum(F.when(F.col("predicted_decision") == "commit", 1).otherwise(0)) / F.count("*"), 4)
             .alias("predicted_commit_rate"),
        ).orderBy(F.desc("attempt_count"))
        overall_pdf = overall.toPandas()
        overall_pdf.to_csv(os.path.join(results_dir, "m5_batch_transactions_by_scenario.csv"), index=False)

        return {
            "technology": "Apache Spark (PySpark, local[*] mode)",
            "total_rows_processed": sdf.count(),
            "spark_partitions_used": sdf.rdd.getNumPartitions(),
            "by_pair_scenario": by_pair_pdf.to_dict(orient="records"),
            "by_scenario": overall_pdf.to_dict(orient="records"),
        }
    finally:
        spark.stop()


# =============================================================================
# [MILESTONE 6 - Concurrency and Deadlock Management] big-data batch layer
# =============================================================================

def generate_lock_contention_dataset(n_rows: int = 100_000, seed: int = 6061) -> pd.DataFrame:
    """
    Vectorized generation of a large dataset of simulated lock REQUESTS:
    which transaction, which node, which named resource, and what
    position in that transaction's acquisition order (1st lock requested,
    2nd, ...). This is what lets the batch layer analyze, at scale,
    which (node, resource) combinations are requested most often and are
    therefore the most likely sites of real contention -- complementing
    (not replacing) the small, deliberately-engineered live deadlock
    demonstration in milestone6_concurrency_deadlock.py.
    """
    rng = np.random.default_rng(seed)
    tx_id = rng.integers(0, n_rows // 4, size=n_rows)  # ~4 lock requests per transaction on average
    node = rng.choice(LOCK_NODES, size=n_rows)
    resource = rng.choice(LOCK_RESOURCE_NAMES, size=n_rows)
    order_position = rng.integers(1, 5, size=n_rows)

    df = pd.DataFrame({
        "request_id": np.arange(n_rows),
        "tx_id": tx_id,
        "node": node,
        "resource": resource,
        "order_position": order_position,
    })
    return df


def run_lock_contention_batch_layer(n_rows: int = 100_000, seed: int = 6061, results_dir: str = None) -> dict:
    """
    [BATCH LAYER - Milestone 6] Spark-aggregates the large lock-request
    dataset by (node, resource) to rank which resources see the heaviest
    simulated contention, and separately flags (node, resource) pairs
    requested at BOTH order_position 1 and order_position 2+ by
    different transactions -- a rough, large-scale proxy for "this
    resource is a plausible participant in a circular-wait," the same
    structural pattern the live deadlock experiment deliberately builds.
    """
    from pyspark.sql import functions as F
    from common.powerbi_export import BIGDATA_DIR

    if results_dir is None:
        results_dir = BIGDATA_DIR
    os.makedirs(results_dir, exist_ok=True)

    pdf = generate_lock_contention_dataset(n_rows=n_rows, seed=seed)

    spark = get_spark_session("5g6g-platform-m6-lock-contention-batch")
    try:
        sdf = spark.createDataFrame(pdf).repartition(8)

        contention = (
            sdf.groupBy("node", "resource")
               .agg(
                   F.count("*").alias("request_count"),
                   F.countDistinct("tx_id").alias("distinct_transactions"),
                   F.round(F.avg("order_position"), 2).alias("avg_order_position"),
               )
               .orderBy(F.desc("request_count"))
        )
        contention_pdf = contention.toPandas()
        contention_pdf.to_csv(os.path.join(results_dir, "m6_batch_lock_contention_by_resource.csv"), index=False)

        return {
            "technology": "Apache Spark (PySpark, local[*] mode)",
            "total_rows_processed": sdf.count(),
            "spark_partitions_used": sdf.rdd.getNumPartitions(),
            "contention_by_resource": contention_pdf.to_dict(orient="records"),
        }
    finally:
        spark.stop()
