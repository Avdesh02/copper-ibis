#!/usr/bin/env python3
"""Run the Spark-dependent tests outside the pytest runner.

On Windows, PySpark's Python workers fail to run inside a pytest process: the worker
socket is reset and every job dies with ``SocketException: Connection reset`` /
``WinError 10038`` (reproduced on pytest 8.3 and 9.2 alike, so it is not a pytest
regression). The identical code runs fine in a plain Python process.

So on Windows ``tests/test_transforms.py`` skips itself, and this script runs the same
tests in a subprocess where they work. On Linux (including CI) pytest runs them directly
and this script is unnecessary.

Usage:  python scripts/run_spark_tests.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def main() -> int:
    env = dict(os.environ)
    env.update(
        {
            "SPARK_LOCAL_HOSTNAME": "localhost",
            "SPARK_LOCAL_IP": "127.0.0.1",
            "PYSPARK_PYTHON": sys.executable,
            "TURBINE_SPARK_TESTS": "1",  # tells conftest the runner is safe
            "PYTHONPATH": os.pathsep.join(
                [str(REPO / "src"), str(REPO / "tests"), env.get("PYTHONPATH", "")]
            ).rstrip(os.pathsep),
        }
    )

    runner = REPO / "tests" / "_spark_runner.py"
    print(f"Running Spark tests via {runner} (outside pytest)\n")
    return subprocess.call([sys.executable, str(runner)], cwd=str(REPO), env=env)


if __name__ == "__main__":
    raise SystemExit(main())
