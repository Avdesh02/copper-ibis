"""Bronze sensor telemetry — Auto Loader ingest with two APPEND FLOWS.

Demonstrates the append-flow fan-in pattern: one target streaming table fed by two
independent flows.

* ``live`` streams newly landed files continuously (Auto Loader).
* ``backfill`` loads the historical seed directory exactly once (``once=True``).

Append flows are the right tool here rather than a UNION: the two sources have different
lifecycles, and each flow keeps its own independent progress.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from turbine_pipeline.lib.config import get_conf
from turbine_pipeline.lib.schemas import RESCUED_COLUMN, SENSOR_EVENT_SCHEMA
from turbine_pipeline.lib.transforms import add_ingest_metadata

SENSORS_PATH = get_conf(spark, "turbine.landing.sensors_path")  # noqa: F821
BACKFILL_PATH = get_conf(spark, "turbine.landing.sensors_backfill_path")  # noqa: F821

# Explicit target: multiple flows write into it, so it cannot be defined by a decorator.
dp.create_streaming_table(
    name="bronze_sensor_raw_copper_ibis",
    comment="Raw turbine sensor events as landed, from the live stream and the one-off backfill.",
    cluster_by=["site_id", "turbine_id"],
    table_properties={"delta.enableChangeDataFeed": "true"},
)


def _read_sensor_files(path: str):
    """Auto Loader read of the landing zone.

    ``cloudFiles.schemaLocation`` is deliberately not set — the pipeline manages the
    schema location itself, and setting it breaks the run.
    """
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", RESCUED_COLUMN)
        .schema(SENSOR_EVENT_SCHEMA)
        .load(path)
    )


@dp.append_flow(target="bronze_sensor_raw_copper_ibis", name="live_sensor_stream")
def live_sensor_stream():
    """Continuous ingest of newly arriving sensor files."""
    return add_ingest_metadata(_read_sensor_files(SENSORS_PATH)).withColumn(
        "_flow_source", F.lit("live")
    )


@dp.append_flow(
    target="bronze_sensor_raw_copper_ibis", name="historical_backfill", once=True
)
def historical_backfill():
    """One-shot load of the seeded history.

    ``once=True`` flows are batch, so this is a plain ``spark.read`` — Auto Loader is
    streaming-only and cannot be used here.
    """
    return (
        spark.read.format("json")  # noqa: F821
        .schema(SENSOR_EVENT_SCHEMA)
        .load(BACKFILL_PATH)
        .transform(add_ingest_metadata)
        .withColumn("_flow_source", F.lit("backfill"))
    )
