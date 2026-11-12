"""Gold enriched readings — streaming table with a stream-static join.

Readings stream in; the turbine dimension is read as a static snapshot of its current
rows. Keeps site, turbine and time so downstream consumers can slice by them.
"""

from pyspark import pipelines as dp

from turbine_pipeline.lib.config import get_conf
from turbine_pipeline.lib.transforms import current_dimension_rows, enrich_readings

SILVER_SCHEMA = get_conf(spark, "turbine.schema.silver", "silver")  # noqa: F821
GOLD_SCHEMA = get_conf(spark, "turbine.schema.gold", "gold")  # noqa: F821


@dp.table(
    name=f"{GOLD_SCHEMA}.gold_reading_enriched_copper_ibis",
    comment="Sensor readings enriched with current turbine attributes.",
    cluster_by=["site_id", "turbine_id", "event_date"],
)
def gold_reading_enriched_copper_ibis():
    readings = spark.readStream.table(f"{SILVER_SCHEMA}.silver_sensor_readings_copper_ibis")  # noqa: F821
    # Batch read: the static side of a stream-static join.
    dim = spark.read.table(f"{SILVER_SCHEMA}.dim_turbine_copper_ibis")  # noqa: F821
    return enrich_readings(readings, current_dimension_rows(dim))
