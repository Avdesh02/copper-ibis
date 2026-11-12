"""Transformation tests against a local Spark session.

These exercise the same functions the SDP pipeline calls, so a logic regression is
caught here rather than in a deployed pipeline run.

**Windows note.** PySpark's Python workers cannot run inside a pytest process on
Windows: the worker socket is reset and every job fails with ``SocketException:
Connection reset`` / ``WinError 10038``. Reproduced on pytest 8.3 and 9.2, so it is not
a pytest regression, and the identical code passes in a plain Python process. These
tests therefore skip on Windows under pytest and are run instead by
``python scripts/run_spark_tests.py``, which executes these very same classes outside
the runner. On Linux CI they run here normally.
"""

from __future__ import annotations

import os
import sys

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")

pytestmark = pytest.mark.skipif(
    sys.platform == "win32" and not os.environ.get("TURBINE_SPARK_TESTS"),
    reason="PySpark workers fail under pytest on Windows; run scripts/run_spark_tests.py",
)

from pyspark.sql import Row  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from turbine_pipeline.lib import transforms  # noqa: E402
from turbine_pipeline.lib.config import Thresholds  # noqa: E402
from turbine_pipeline.lib.schemas import SENSOR_EVENT_SCHEMA  # noqa: E402

# Nullable columns give Spark nothing to infer from, so test frames declare schemas.
_READING_VALUE_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), True),
        StructField("metric", StringType(), True),
        StructField("value", DoubleType(), True),
    ]
)


def _event(
    event_id="e1",
    turbine="TURB-001",
    site="rajasthan",
    ts="2026-01-01T10:00:00",
    rescued=None,
    readings=None,
):
    if readings is None:
        readings = [
            Row(metric="oil_temperature", value=88.0, unit="celsius", quality="GOOD"),
            Row(metric="blade_velocity", value=13.5, unit="rpm", quality="GOOD"),
        ]
    return Row(
        event_id=event_id,
        turbine_id=turbine,
        site_id=site,
        event_ts=ts,
        seq=1,
        wind_speed_mps=9.0,
        readings=readings,
        _rescued_data=rescued,
    )


def _events_df(spark, rows):
    """Build a bronze-shaped frame with the real schema and ingest metadata."""
    return (
        spark.createDataFrame(rows, schema=SENSOR_EVENT_SCHEMA)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit("f.json"))
    )


class TestQuarantineRouting:
    def test_clean_and_malformed_rows_are_partitioned(self, spark):
        df = _events_df(spark, [_event("good"), _event("bad", rescued='{"x":1}')])
        flagged = transforms.flag_malformed(df)

        clean = [r.event_id for r in transforms.select_clean(flagged).collect()]
        quarantined = [r.event_id for r in transforms.select_malformed(flagged).collect()]

        assert clean == ["good"]
        assert quarantined == ["bad"]

    def test_partition_is_exhaustive(self, spark):
        """Every row lands in exactly one branch — nothing is silently lost."""
        df = _events_df(spark, [_event("a"), _event("b", rescued="{}"), _event("c")])
        flagged = transforms.flag_malformed(df)
        total = flagged.count()
        assert (
            transforms.select_clean(flagged).count()
            + transforms.select_malformed(flagged).count()
            == total
        )


class TestExplodeReadings:
    def test_one_row_per_metric(self, spark):
        df = _events_df(spark, [_event()])

        out = transforms.explode_readings(df).collect()

        assert len(out) == 2
        assert {r.metric for r in out} == {"oil_temperature", "blade_velocity"}

    def test_parses_timestamp_and_derives_date(self, spark):
        df = _events_df(spark, [_event(ts="2026-03-04T05:06:07")])

        row = transforms.explode_readings(df).first()

        assert row.event_ts.year == 2026
        assert str(row.event_date) == "2026-03-04"

    def test_event_with_no_readings_is_retained(self, spark):
        """explode_outer keeps the event so an empty payload is still observable."""
        df = _events_df(spark, [_event(event_id="e0", readings=[])])

        out = transforms.explode_readings(df).collect()

        assert len(out) == 1
        assert out[0].metric is None


class TestThresholdsAndSeverity:
    THRESHOLDS = {
        "rajasthan": {
            "oil_temperature": Thresholds(warn_max=92.0, critical_max=100.0, warn_min=5.0)
        },
        "karnataka": {
            "oil_temperature": Thresholds(warn_max=80.0, critical_max=95.0)
        },
    }

    def _frame(self, spark, rows):
        return spark.createDataFrame(
            [Row(site_id=s, metric=m, value=v) for s, m, v in rows],
            schema=_READING_VALUE_SCHEMA,
        )

    def test_thresholds_are_site_specific(self, spark):
        """The same reading is fine in a hot site and a warning in a cool one."""
        df = self._frame(
            spark,
            [("rajasthan", "oil_temperature", 85.0), ("karnataka", "oil_temperature", 85.0)],
        )
        out = transforms.classify_severity(
            transforms.attach_thresholds(df, self.THRESHOLDS)
        ).collect()
        severity = {r.site_id: r.severity for r in out}

        assert severity["rajasthan"] == "OK"
        assert severity["karnataka"] == "WARN"

    def test_critical_beats_warn(self, spark):
        df = self._frame(spark, [("rajasthan", "oil_temperature", 500.0)])
        row = transforms.classify_severity(
            transforms.attach_thresholds(df, self.THRESHOLDS)
        ).first()
        assert row.severity == "CRITICAL"

    def test_below_warn_min_warns(self, spark):
        df = self._frame(spark, [("rajasthan", "oil_temperature", 1.0)])
        row = transforms.classify_severity(
            transforms.attach_thresholds(df, self.THRESHOLDS)
        ).first()
        assert row.severity == "WARN"

    def test_unconfigured_metric_never_alerts(self, spark):
        df = self._frame(spark, [("rajasthan", "unknown", 1e9)])
        row = transforms.classify_severity(
            transforms.attach_thresholds(df, self.THRESHOLDS)
        ).first()
        assert row.severity == "OK"

    def test_null_value_is_ok(self, spark):
        df = self._frame(spark, [("rajasthan", "oil_temperature", None)])
        row = transforms.classify_severity(
            transforms.attach_thresholds(df, self.THRESHOLDS)
        ).first()
        assert row.severity == "OK"

    def test_select_alerts_drops_ok_rows(self, spark):
        df = self._frame(
            spark,
            [
                ("karnataka", "oil_temperature", 10.0),   # OK
                ("karnataka", "oil_temperature", 85.0),   # WARN
                ("karnataka", "oil_temperature", 99.0),   # CRITICAL
            ],
        )
        alerts = transforms.select_alerts(
            transforms.classify_severity(transforms.attach_thresholds(df, self.THRESHOLDS))
        ).collect()

        assert sorted(r.severity for r in alerts) == ["CRITICAL", "WARN"]


class TestScd2Dimension:
    def test_only_current_rows_are_kept(self, spark):
        scd2_schema = StructType(
            [
                StructField("turbine_id", StringType(), True),
                StructField("status", StringType(), True),
                StructField("__START_AT", StringType(), True),
                StructField("__END_AT", StringType(), True),
            ]
        )
        dim = spark.createDataFrame(
            [
                Row("T1", "active", "2026-01-01", None),
                Row("T1", "maintenance", "2025-01-01", "2026-01-01"),
            ],
            schema=scd2_schema,
        )
        out = transforms.current_dimension_rows(dim).collect()

        assert len(out) == 1
        assert out[0].status == "active"

    def test_passthrough_when_not_scd2(self, spark):
        dim = spark.createDataFrame([Row(turbine_id="T1", status="active")])
        assert transforms.current_dimension_rows(dim).count() == 1


class TestEnrichReadings:
    _READINGS_SCHEMA = StructType(
        [
            StructField("turbine_id", StringType(), True),
            StructField("site_id", StringType(), True),
            StructField("metric", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )
    _DIM_SCHEMA = StructType(
        [
            StructField("turbine_id", StringType(), True),
            StructField("capacity_mw", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("hub_height_m", DoubleType(), True),
            StructField("rotor_diameter_m", DoubleType(), True),
            StructField("__END_AT", StringType(), True),
        ]
    )

    def _dim(self, spark):
        return spark.createDataFrame(
            [Row("TURB-001", 2.1, "active", 100.0, 80.0, None)], schema=self._DIM_SCHEMA
        )

    def test_joins_capacity_from_dimension(self, spark):
        readings = spark.createDataFrame(
            [Row("TURB-001", "rajasthan", "m", 1.0)], schema=self._READINGS_SCHEMA
        )

        row = transforms.enrich_readings(
            readings, transforms.current_dimension_rows(self._dim(spark))
        ).first()

        assert row.capacity_mw == 2.1
        assert row.turbine_status == "active"

    def test_left_join_keeps_unmatched_readings(self, spark):
        """A reading from an unknown turbine must not vanish."""
        readings = spark.createDataFrame(
            [Row("GHOST", "s", "m", 1.0)], schema=self._READINGS_SCHEMA
        )

        out = transforms.enrich_readings(
            readings, transforms.current_dimension_rows(self._dim(spark))
        ).collect()

        assert len(out) == 1
        assert out[0].capacity_mw is None


class TestWindowAggregate:
    def test_groups_into_tumbling_windows(self, spark):
        rows = [
            Row(site_id="s", turbine_id="T", metric="m", value=10.0,
                wind_speed_mps=5.0, event_ts="2026-01-01T10:00:00"),
            Row(site_id="s", turbine_id="T", metric="m", value=20.0,
                wind_speed_mps=7.0, event_ts="2026-01-01T10:02:00"),
            Row(site_id="s", turbine_id="T", metric="m", value=99.0,
                wind_speed_mps=9.0, event_ts="2026-01-01T11:30:00"),
        ]
        df = spark.createDataFrame(rows).withColumn("event_ts", F.to_timestamp("event_ts"))

        out = transforms.window_aggregate(df, "5 minutes").collect()

        assert len(out) == 2
        first = min(out, key=lambda r: r.window_start)
        assert first.reading_count == 2
        assert first.avg_value == pytest.approx(15.0)
        assert first.min_value == 10.0
        assert first.max_value == 20.0


class TestLatestState:
    def test_keeps_only_most_recent_per_turbine_metric(self, spark):
        rows = [
            Row(site_id="s", turbine_id="T", metric="m", value=1.0, unit="u",
                quality="GOOD", event_ts="2026-01-01T10:00:00"),
            Row(site_id="s", turbine_id="T", metric="m", value=2.0, unit="u",
                quality="GOOD", event_ts="2026-01-01T12:00:00"),
        ]
        df = spark.createDataFrame(rows).withColumn("event_ts", F.to_timestamp("event_ts"))

        out = transforms.latest_state_per_turbine(df).collect()

        assert len(out) == 1
        assert out[0].value == 2.0
