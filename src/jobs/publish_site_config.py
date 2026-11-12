"""Seed the site control table and publish the active-site list.

Two jobs in one:

1. Write ``ops.site_config_copper_ibis`` — the runtime half of the parameterization.
   Thresholds can be edited there and take effect on the next run, with no redeploy.
   A ``pipeline_task`` cannot pass per-run parameters, so this table is how runtime
   values reach the pipeline.

2. Publish the enabled site ids as a task value, which the orchestrator's
   ``for_each_task`` fans out over. The roster is static, but which sites actually run
   is decided here, at run time.
"""

from __future__ import annotations

import argparse
import json
import sys
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
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from turbine_pipeline.lib.sites import ALL_SITE_IDS, SITES  # noqa: E402

SITE_CONFIG_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), False),
        StructField("state", StringType(), False),
        StructField("turbine_id", StringType(), False),
        StructField("capacity_mw", DoubleType(), False),
        StructField("wind_factor", DoubleType(), False),
        StructField("temp_base_c", DoubleType(), False),
        StructField("enabled", BooleanType(), False),
    ]
)


def build_rows(disabled: set[str]) -> list[tuple]:
    rows = []
    for site_id in ALL_SITE_IDS:
        site = SITES[site_id]
        for turbine in site.turbines:
            rows.append(
                (
                    site_id,
                    site.state,
                    turbine.turbine_id,
                    float(turbine.capacity_mw),
                    float(site.wind_factor),
                    float(site.temp_base_c),
                    site_id not in disabled,
                )
            )
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--ops-schema", default="ops")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--run-id", default="local")
    parser.add_argument(
        "--disable-sites",
        default="",
        help="comma-separated site ids to exclude from this run",
    )
    args, _unknown = parser.parse_known_args(argv)

    disabled = {s.strip() for s in args.disable_sites.split(",") if s.strip()}
    spark = SparkSession.builder.getOrCreate()

    table = f"{args.catalog}.{args.ops_schema}.site_config_copper_ibis"
    df = spark.createDataFrame(build_rows(disabled), schema=SITE_CONFIG_SCHEMA).withColumn(
        "updated_at", F.current_timestamp()
    )
    # Overwrite: this table is the declared desired state, not an event log.
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

    active = [r.site_id for r in df.filter("enabled").select("site_id").distinct().collect()]
    active.sort()

    print(f"wrote {df.count()} rows to {table}")
    print(f"active sites ({len(active)}): {', '.join(active)}")

    # for_each_task reads `inputs` as a JSON array, so publish it as one.
    try:
        dbutils.jobs.taskValues.set(key="active_sites", value=active)  # noqa: F821
        dbutils.jobs.taskValues.set(key="site_count", value=len(active))  # noqa: F821
    except Exception:
        print(f"(not on Databricks) active_sites={json.dumps(active)}")

    return 0


if __name__ == "__main__":
    # A serverless spark_python_task runs this file through exec(), where even
    # SystemExit(0) is reported as a task failure. So exit explicitly only when the
    # work actually failed, and otherwise just fall off the end.
    _rc = main()
    if _rc:
        raise SystemExit(_rc)
