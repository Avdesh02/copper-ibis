"""Post-pipeline validation for one site.

Runs inside the orchestrator's second for_each fan-out, once per site, so ``--site`` is
the per-item ``{{input}}``. Asserts the gold layer actually produced data for that site
rather than assuming a green pipeline means good output.

Exits non-zero when a site produced no enriched readings, which surfaces as a failed
iteration while the other sites continue.
"""

from __future__ import annotations

import argparse
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--gold-schema", default="gold")
    parser.add_argument("--run-id", default="local")
    parser.add_argument(
        "--require-rows",
        type=int,
        default=1,
        help="minimum enriched rows for the site before this check passes",
    )
    args, _unknown = parser.parse_known_args(argv)

    spark = SparkSession.builder.getOrCreate()
    site = args.site
    cat = args.catalog

    def count(table: str, where: str) -> int:
        try:
            return spark.table(f"{cat}.{table}").filter(where).count()
        except Exception as exc:  # table may not exist on a first partial run
            print(f"  ! could not read {table}: {exc}")
            return -1

    gold = args.gold_schema
    enriched = count(f"{gold}.gold_reading_enriched_copper_ibis", f"site_id = '{site}'")
    alerts = count(f"{gold}.gold_threshold_alerts_copper_ibis", f"site_id = '{site}'")
    windows = count(f"{gold}.gold_site_window_agg_copper_ibis", f"site_id = '{site}'")

    print(f"site={site}")
    print(f"  gold_reading_enriched : {enriched}")
    print(f"  gold_threshold_alerts : {alerts}")
    print(f"  gold_site_window_agg  : {windows}")

    try:
        dbutils.jobs.taskValues.set(key=f"enriched_{site}", value=enriched)  # noqa: F821
        dbutils.jobs.taskValues.set(key=f"alerts_{site}", value=alerts)  # noqa: F821
    except Exception:
        pass

    if enriched < args.require_rows:
        print(f"FAIL: expected >= {args.require_rows} enriched rows for {site}, got {enriched}")
        return 1

    print(f"OK: {site} validated")
    return 0


if __name__ == "__main__":
    # A serverless spark_python_task runs this file through exec(), where even
    # SystemExit(0) is reported as a task failure. So exit explicitly only when the
    # work actually failed, and otherwise just fall off the end.
    _rc = main()
    if _rc:
        raise SystemExit(_rc)
