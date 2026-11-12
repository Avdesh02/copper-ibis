"""Gold windowed aggregates — MATERIALIZED VIEW.

A materialized view, not a streaming table: streaming tables are append-only and never
recompute an aggregate when late data lands, whereas an MV recomputes (incrementally on
serverless). Aggregation over a streaming source therefore reads it in **batch** mode.

Window size comes from pipeline configuration, so it is tunable per environment without
touching this code.
"""

from pyspark import pipelines as dp

from turbine_pipeline.lib.config import get_conf
from turbine_pipeline.lib.transforms import window_aggregate

WINDOW_DURATION = get_conf(spark, "turbine.window_duration", "5 minutes")  # noqa: F821


SILVER_SCHEMA = get_conf(spark, "turbine.schema.silver", "silver")  # noqa: F821
GOLD_SCHEMA = get_conf(spark, "turbine.schema.gold", "gold")  # noqa: F821


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_site_window_agg_copper_ibis",
    comment=f"Tumbling {WINDOW_DURATION} aggregates per site, turbine and metric.",
    cluster_by=["site_id", "metric"],
)
def gold_site_window_agg_copper_ibis():
    readings = spark.read.table(f"{SILVER_SCHEMA}.silver_sensor_readings_copper_ibis")  # noqa: F821
    return window_aggregate(readings, WINDOW_DURATION)
