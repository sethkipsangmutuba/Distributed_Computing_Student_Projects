"""
powerbi_export.py
------------------
[Big Data / Power BI integration - shared across EVERY assignment]

This is the single, consistent export layer every assignment's big-data
extension script writes through, so the whole 12-week project builds
ONE growing Power BI dashboard rather than a separate disconnected
dashboard per assignment. Two project-ROOT-level folders (not one per
assignment) are used:

    results/bigdata/   -- raw Spark batch-layer outputs (large datasets,
                           full aggregation tables)
    results/powerbi/   -- flattened, Power-BI-ready CSVs, one file per
                           chart/table the dashboard guide describes.
                           File names are prefixed by which milestone(s)
                           produced them (e.g. m3_scalability.csv,
                           m5_transaction_summary.csv) so files from
                           different assignments coexist in the SAME
                           folder without colliding, and Power BI can
                           import the whole folder's growth over time
                           without needing to be rewired each assignment.

Every assignment's *_bigdata_extension.py script should:
    1. import PROJECT_ROOT, POWERBI_DIR, BIGDATA_DIR from this module
    2. write its Spark/batch outputs under BIGDATA_DIR
    3. write its Power-BI-ready flat CSVs under POWERBI_DIR, using
       write_csv() below, with an "m<N>_" filename prefix
This keeps the pattern identical assignment to assignment.
"""

import os
import csv
import json

# Project root = the directory containing common/, assignment1_m1_m2/, etc.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
POWERBI_DIR = os.path.join(PROJECT_ROOT, "results", "powerbi")
BIGDATA_DIR = os.path.join(PROJECT_ROOT, "results", "bigdata")

os.makedirs(POWERBI_DIR, exist_ok=True)
os.makedirs(BIGDATA_DIR, exist_ok=True)


def write_csv(filename: str, rows: list, fieldnames: list = None) -> str:
    """
    Writes `rows` (a list of dicts) to results/powerbi/<filename>.
    fieldnames is inferred from the first row if not given. Returns the
    full path written, for logging.
    """
    path = os.path.join(POWERBI_DIR, filename)
    if not rows:
        # Still write an empty file with a header if we can infer one,
        # so Power BI's "Get Data" doesn't error out on a missing file
        # if this export ran with no matching data this time.
        with open(path, "w", newline="") as f:
            if fieldnames:
                csv.writer(f).writerow(fieldnames)
        return path
    if fieldnames is None:
        fieldnames = list(rows[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def write_metric_rows_csv(filename: str, metrics: dict) -> str:
    """Writes a flat {metric_name: value} dict as a two-column metric/value CSV."""
    rows = [{"metric": k, "value": v} for k, v in metrics.items()]
    return write_csv(filename, rows, fieldnames=["metric", "value"])


def flatten_json_result(json_path: str, row_builder) -> list:
    """
    Loads a milestone script's saved JSON results file and applies
    `row_builder(data) -> list[dict]` to turn it into flat rows ready
    for write_csv(). Returns [] (and logs nothing itself -- caller logs)
    if the JSON file doesn't exist yet, so an extension script can be
    run even if an earlier milestone script hasn't been run this session.
    """
    if not os.path.exists(json_path):
        return []
    with open(json_path) as f:
        data = json.load(f)
    return row_builder(data)
