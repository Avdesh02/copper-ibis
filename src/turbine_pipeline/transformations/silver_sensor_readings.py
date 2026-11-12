"""Silver sensor readings — streaming flow, expectations, and a quarantine branch.

Bronze events are exploded into long/narrow rows, then split: parseable rows go to the
clean table, rows Auto Loader had to rescue go to quarantine. Quarantining rather than
dropping keeps schema drift visible instead of silently losing data.
"""

from pyspark import pipelines as dp

from turbine_pipeline.lib.config import get_conf
from turbine_pipeline.lib.transforms import (
    explode_readings,
    flag_malformed,
    select_clean,
    select_malformed,
)

BRONZE_SCHEMA = get_conf(spark, "turbine.schema.bronze", "bronze")  # noqa: F821
SILVER_SCHEMA = get_conf(spark, "turbine.schema.silver", "silver")  # noqa: F821


@dp.table(
    name=f"{SILVER_SCHEMA}.silver_sensor_readings_copper_ibis",
    comment="Validated long/narrow sensor readings, one row per metric per event.",
    cluster_by=["site_id", "turbine_id", "event_date"],
    table_properties={"delta.enableChangeDataFeed": "true"},
)
# Structural rules: a row without these is unusable downstream, so drop it.
@dp.expect_all_or_drop(
    {
        "turbine_id_present": "turbine_id IS NOT NULL",
        "site_id_present": "site_id IS NOT NULL",
        "event_ts_parsed": "event_ts IS NOT NULL",
        "metric_present": "metric IS NOT NULL",
    }
)
# Value rules: warn only. A sensor reporting an implausible number is itself a signal
# worth keeping, and dropping it would hide the fault.
@dp.expect_all(
    {
        "value_present": "value IS NOT NULL",
        "quality_known": "quality IN ('GOOD', 'SUSPECT', 'BAD') OR quality IS NULL",
        "wind_speed_plausible": "wind_speed_mps IS NULL OR (wind_speed_mps >= 0 AND wind_speed_mps <= 60)",
    }
)
def silver_sensor_readings_copper_ibis():
    bronze = spark.readStream.table(f"{BRONZE_SCHEMA}.bronze_sensor_raw_copper_ibis")  # noqa: F821
    clean = select_clean(flag_malformed(bronze))
    return explode_readings(clean)


@dp.table(
    name=f"{SILVER_SCHEMA}.silver_sensor_quarantine_copper_ibis",
    comment="Sensor events Auto Loader could not fully parse; inspect to spot schema drift.",
    cluster_by=["site_id"],
)
def silver_sensor_quarantine_copper_ibis():
    bronze = spark.readStream.table(f"{BRONZE_SCHEMA}.bronze_sensor_raw_copper_ibis")  # noqa: F821
    return select_malformed(flag_malformed(bronze))
