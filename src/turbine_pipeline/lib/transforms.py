"""DataFrame transformations.

Every function here is ``DataFrame -> DataFrame`` with no side effects, so the pipeline's
real logic can be unit-tested against a local Spark session. The ``transformations/``
modules are thin ``@dp`` wrappers that call into this module and nothing else.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .config import Thresholds
from .schemas import RESCUED_COLUMN

# Metrics that carry alert thresholds.
ALERTABLE_METRICS = ("oil_temperature", "blade_velocity", "gear_friction")


def add_ingest_metadata(df: DataFrame) -> DataFrame:
    """Stamp ingest-time provenance onto a raw bronze row."""
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.col("_metadata.file_path")
    )


def flag_malformed(df: DataFrame) -> DataFrame:
    """Mark rows Auto Loader could not fully parse, without dropping them."""
    rescued = F.col(RESCUED_COLUMN) if RESCUED_COLUMN in df.columns else F.lit(None)
    return df.withColumn("_has_errors", rescued.isNotNull())


def select_malformed(df: DataFrame) -> DataFrame:
    """Rows bound for quarantine."""
    return df.filter(F.col("_has_errors"))


def select_clean(df: DataFrame) -> DataFrame:
    """Rows safe to promote to silver."""
    return df.filter(~F.col("_has_errors"))


def explode_readings(df: DataFrame) -> DataFrame:
    """Turn one wide event into one long/narrow row per metric.

    Long/narrow keeps the silver table stable as metrics come and go, and makes the
    per-metric threshold join in gold a plain equi-join.
    """
    exploded = df.withColumn("reading", F.explode_outer("readings"))
    return exploded.select(
        F.col("event_id"),
        F.col("turbine_id"),
        F.col("site_id"),
        F.to_timestamp("event_ts").alias("event_ts"),
        F.col("seq"),
        F.col("wind_speed_mps"),
        F.col("reading.metric").alias("metric"),
        F.col("reading.value").cast("double").alias("value"),
        F.col("reading.unit").alias("unit"),
        F.col("reading.quality").alias("quality"),
        F.col("_ingested_at"),
        F.col("_source_file"),
    ).withColumn("event_date", F.to_date("event_ts"))


def _threshold_column(
    site_thresholds: dict[str, dict[str, Thresholds]], bound: str
) -> Column:
    """Build a CASE expression mapping (site_id, metric) to one threshold bound.

    Thresholds come from pipeline configuration — static at deploy time — so folding
    them into a literal expression avoids a shuffle that a join would cost.
    """
    expr = None
    for site_id, per_metric in sorted(site_thresholds.items()):
        for metric, thresholds in sorted(per_metric.items()):
            value = getattr(thresholds, bound)
            if value is None:
                continue
            cond = (F.col("site_id") == F.lit(site_id)) & (F.col("metric") == F.lit(metric))
            expr = F.when(cond, F.lit(float(value))) if expr is None else expr.when(
                cond, F.lit(float(value))
            )
    return F.lit(None).cast("double") if expr is None else expr.otherwise(F.lit(None).cast("double"))


def attach_thresholds(
    df: DataFrame, site_thresholds: dict[str, dict[str, Thresholds]]
) -> DataFrame:
    """Add the four threshold bounds for each row's site and metric."""
    out = df
    for bound in ("warn_min", "warn_max", "critical_min", "critical_max"):
        out = out.withColumn(bound, _threshold_column(site_thresholds, bound))
    return out


def classify_severity(df: DataFrame) -> DataFrame:
    """Derive CRITICAL / WARN / OK from the attached bounds.

    Critical is evaluated before warn so an extreme reading is never downgraded.
    """
    value = F.col("value")
    critical = (
        (F.col("critical_max").isNotNull() & (value > F.col("critical_max")))
        | (F.col("critical_min").isNotNull() & (value < F.col("critical_min")))
    )
    warn = (
        (F.col("warn_max").isNotNull() & (value > F.col("warn_max")))
        | (F.col("warn_min").isNotNull() & (value < F.col("warn_min")))
    )
    return df.withColumn(
        "severity",
        F.when(value.isNull(), F.lit("OK"))
        .when(critical, F.lit("CRITICAL"))
        .when(warn, F.lit("WARN"))
        .otherwise(F.lit("OK")),
    )


def select_alerts(df: DataFrame) -> DataFrame:
    """Keep only rows that breached a bound."""
    return df.filter(F.col("severity") != F.lit("OK"))


def current_dimension_rows(dim: DataFrame) -> DataFrame:
    """Current rows of an SCD2 dimension.

    Lakeflow writes ``__END_AT`` as NULL for the live version of each key.
    """
    if "__END_AT" not in dim.columns:
        return dim
    return dim.filter(F.col("__END_AT").isNull())


def enrich_readings(readings: DataFrame, dim_current: DataFrame) -> DataFrame:
    """Join readings to the current turbine dimension (stream-static join)."""
    dim = dim_current.select(
        F.col("turbine_id").alias("_dim_turbine_id"),
        F.col("capacity_mw"),
        F.col("status").alias("turbine_status"),
        F.col("hub_height_m"),
        F.col("rotor_diameter_m"),
    )
    return readings.join(
        dim, readings["turbine_id"] == dim["_dim_turbine_id"], "left"
    ).drop("_dim_turbine_id")


def window_aggregate(df: DataFrame, window_duration: str) -> DataFrame:
    """Tumbling-window stats per site, turbine and metric.

    Aggregated from a batch read in a materialized view: streaming tables are
    append-only and would never recompute these as late data lands.
    """
    return (
        df.groupBy(
            F.col("site_id"),
            F.col("turbine_id"),
            F.col("metric"),
            F.window(F.col("event_ts"), window_duration).alias("time_window"),
        )
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count(F.lit(1)).alias("reading_count"),
            F.avg("wind_speed_mps").alias("avg_wind_speed_mps"),
        )
        .select(
            "site_id",
            "turbine_id",
            "metric",
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "avg_value",
            "min_value",
            "max_value",
            "reading_count",
            "avg_wind_speed_mps",
        )
    )


def latest_state_per_turbine(df: DataFrame) -> DataFrame:
    """Most recent reading per turbine and metric."""
    latest_ts = df.groupBy("turbine_id", "metric").agg(
        F.max("event_ts").alias("event_ts")
    )
    return df.join(latest_ts, ["turbine_id", "metric", "event_ts"], "inner").select(
        "site_id",
        "turbine_id",
        "metric",
        "event_ts",
        "value",
        "unit",
        "quality",
    )
