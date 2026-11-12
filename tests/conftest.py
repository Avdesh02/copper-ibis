"""Pytest fixtures.

The environment setup below must run **before** any SparkSession is created, which is
why it sits at import time rather than inside the fixture.

On this Windows host Docker Desktop adds ``host.docker.internal`` /
``kubernetes.docker.internal`` entries to the hosts file. Spark resolves the driver to
one of those, the Python worker then cannot connect back, and every job dies with
"Python worker failed to connect back ... Accept timed out". Pinning the hostname to
``localhost`` avoids it. Harmless on Linux CI.
"""

from __future__ import annotations

import os
import sys

os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession shared across the test session.

    Session-scoped because JVM startup costs seconds; the tests only read DataFrames,
    so there is no shared state to leak between them.
    """
    pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("turbine-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    _ = pyspark
    yield session
    session.stop()
