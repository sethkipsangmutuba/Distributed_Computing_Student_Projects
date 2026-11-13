"""
spark_utils.py
---------------
[BIG DATA FRAMEWORK - shared across every assignment, starting Assignment 2]

One shared way to start Spark and run a distributed aggregation, so
every assignment's big-data batch layer (Assignment 2's 100K-row
workload, Assignment 3's 100K-row transaction/lock dataset, and every
assignment after that) uses the EXACT SAME mechanics instead of each
one reinventing Spark setup. This is what "integrated consistently"
means in code: one function, reused everywhere, rather than copy-pasted
and subtly drifting apart assignment to assignment.
"""

import pandas as pd


def get_spark_session(app_name: str = "5g6g-platform-batch-layer"):
    """
    Starts (or reuses) a local-mode Spark session. Every assignment's
    batch layer calls this exact function -- one shared configuration
    (memory, UI disabled for speed) instead of each script configuring
    Spark slightly differently.
    """
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


def run_distributed_aggregation(pdf: pd.DataFrame, group_cols, agg_exprs, n_partitions: int = 8,
                                 app_name: str = "5g6g-platform-batch-layer") -> pd.DataFrame:
    """
    Generic distributed batch aggregation, reusable by ANY assignment's
    dataset: loads a pandas DataFrame into Spark, explicitly repartitions
    it across `n_partitions` (so processing is genuinely parallel
    regardless of how many CPU cores happen to be free), groups by
    `group_cols`, applies `agg_exprs` (a list of pyspark.sql.functions
    expressions), and returns the result as a pandas DataFrame.

    Example:
        from pyspark.sql import functions as F
        run_distributed_aggregation(
            pdf, group_cols=["service_class"],
            agg_exprs=[F.count("*").alias("count"), F.avg("cpu").alias("avg_cpu")],
        )
    """
    spark = get_spark_session(app_name)
    try:
        sdf = spark.createDataFrame(pdf)
        sdf = sdf.repartition(n_partitions)
        result = sdf.groupBy(*group_cols).agg(*agg_exprs)
        return result.toPandas()
    finally:
        spark.stop()
