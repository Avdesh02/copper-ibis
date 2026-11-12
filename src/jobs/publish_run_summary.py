"""Record one row per orchestrator run into the ops audit table.

Every value here except the catalog arrives as a Databricks **dynamic value reference**
resolved at run time — job id, run id, trigger type, repair count, start date, and the
upstream tasks' result states. This task runs with ``run_if: ALL_DONE`` so a run is
audited even when an upstream task failed.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path


# Make `turbine_pipeline` importable.
#
# A serverless spark_python_task runs this file through exec(), so __file__ is not
# defined and the working directory is not the bundle root. The bundle's own source
# root is therefore located explicitly: the task's entry path is visible in
# sys.argv[0], and the deployed layout always puts the package under <root>/src.
def _add_package_root() -> None:
    candidates = []
    try:
        candidates.append(Path(__file__).resolve().parents[1])
    except NameError:
        pass
    if sys.argv and sys.argv[0]:
        entry = Path(sys.argv[0]).resolve()
        candidates.extend([entry.parent.parent, *entry.parents])
    cwd = Path.cwd()
    candidates.extend([cwd / "src", cwd, *cwd.parents])

    for base in candidates:
        for root in (base, base / "src"):
            if (root / "turbine_pipeline").is_dir():
                if str(root) not in sys.path:
                    sys.path.insert(0, str(root))
                return


_add_package_root()

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

AUDIT_SCHEMA = StructType(
    [
        StructField("job_id", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("trigger_type", StringType(), True),
        StructField("repair_count", StringType(), True),
        StructField("processing_date", StringType(), True),
        StructField("env", StringType(), True),
        StructField("task_name", StringType(), True),
        StructField("site_count", IntegerType(), True),
        StructField("config_state", StringType(), True),
        StructField("simulate_state", StringType(), True),
        StructField("pipeline_state", StringType(), True),
        StructField("validate_state", StringType(), True),
        StructField("recorded_at", TimestampType(), True),
    ]
)


def _to_int(raw: str | None) -> int | None:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--ops-schema", default="ops")
    parser.add_argument("--env", default="dev")
    # Each of these is passed in from a {{...}} dynamic value reference.
    parser.add_argument("--job-id", default="")
    parser.add_argument("--job-name", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--trigger-type", default="")
    parser.add_argument("--repair-count", default="")
    parser.add_argument("--processing-date", default="")
    parser.add_argument("--task-name", default="")
    parser.add_argument("--site-count", default="")
    parser.add_argument("--config-state", default="")
    parser.add_argument("--simulate-state", default="")
    parser.add_argument("--pipeline-state", default="")
    parser.add_argument("--validate-state", default="")
    args, _unknown = parser.parse_known_args(argv)

    spark = SparkSession.builder.getOrCreate()
    table = f"{args.catalog}.{args.ops_schema}.run_audit_copper_ibis"

    row = (
        args.job_id,
        args.job_name,
        args.run_id,
        args.trigger_type,
        args.repair_count,
        args.processing_date,
        args.env,
        args.task_name,
        _to_int(args.site_count),
        args.config_state,
        args.simulate_state,
        args.pipeline_state,
        args.validate_state,
        datetime.now(timezone.utc),
    )

    spark.createDataFrame([row], schema=AUDIT_SCHEMA).write.mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(table)

    print(f"audit row written to {table}")
    for field, value in zip(AUDIT_SCHEMA.fieldNames(), row, strict=True):
        print(f"  {field:16} = {value}")

    return 0


if __name__ == "__main__":
    # A serverless spark_python_task runs this file through exec(), where even
    # SystemExit(0) is reported as a task failure. So exit explicitly only when the
    # work actually failed, and otherwise just fall off the end.
    _rc = main()
    if _rc:
        raise SystemExit(_rc)
