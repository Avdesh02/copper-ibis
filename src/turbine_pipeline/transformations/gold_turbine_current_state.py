"""Gold current state — MATERIALIZED VIEW.

The latest reading per turbine and metric. A recomputed snapshot rather than an append
stream, so a materialized view is the right type: each refresh replaces the previous
answer instead of adding to it.
"""

from pyspark import pipelines as dp

from turbine_pipeline.lib.config import get_conf
from turbine_pipeline.lib.transforms import latest_state_per_turbine

SILVER_SCHEMA = get_conf(spark, "turbine.schema.silver", "silver")  # noqa: F821
GOLD_SCHEMA = get_conf(spark, "turbine.schema.gold", "gold")  # noqa: F821


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_turbine_current_state_copper_ibis",
    comment="Most recent reading per turbine and metric.",
    cluster_by=["site_id"],
)
def gold_turbine_current_state_copper_ibis():
    readings = spark.read.table(f"{SILVER_SCHEMA}.silver_sensor_readings_copper_ibis")  # noqa: F821
    return latest_state_per_turbine(readings)
