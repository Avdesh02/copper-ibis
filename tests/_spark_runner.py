"""Plain-Python runner for the Spark transform tests.

Collects the ``Test*`` classes from ``test_transforms.py`` and runs their ``test_*``
methods against a single local SparkSession, reporting pass/fail per test. Exists
because PySpark workers cannot run inside a pytest process on Windows — see
``scripts/run_spark_tests.py`` for the detail.

On Linux the same tests run under pytest directly; this runner is a Windows fallback,
not a second source of truth. It imports the very same test classes, so there is no
risk of the two drifting apart.
"""

from __future__ import annotations

import inspect
import os
import sys
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / "src"), str(REPO / "tests")]

os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ["TURBINE_SPARK_TESTS"] = "1"


def build_spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("turbine-spark-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def main() -> int:
    import test_transforms as mod

    spark = build_spark()
    passed, failed = 0, []

    classes = [
        obj
        for name, obj in vars(mod).items()
        if name.startswith("Test") and inspect.isclass(obj)
    ]

    for cls in classes:
        instance = cls()
        methods = [m for m in dir(instance) if m.startswith("test_")]
        for method_name in sorted(methods):
            label = f"{cls.__name__}::{method_name}"
            try:
                getattr(instance, method_name)(spark)
                passed += 1
                print(f"  PASS  {label}")
            except Exception:
                failed.append((label, traceback.format_exc()))
                print(f"  FAIL  {label}")

    spark.stop()

    print(f"\n{passed} passed, {len(failed)} failed")
    for label, tb in failed:
        print(f"\n--- {label} ---\n{tb}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
