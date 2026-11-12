"""Bronze turbine change feed — Auto Loader ingest of master-data changes.

Feeds the SCD2 dimension built in ``silver_dim_turbine.py``.
"""

from pyspark import pipelines as dp

from turbine_pipeline.lib.config import get_conf
from turbine_pipeline.lib.schemas import RESCUED_COLUMN, TURBINE_CDC_SCHEMA
from turbine_pipeline.lib.transforms import add_ingest_metadata

TURBINES_PATH = get_conf(spark, "turbine.landing.turbines_path")  # noqa: F821


@dp.table(
    name="bronze_turbine_cdc_copper_ibis",
    comment="Turbine master-data change events (UPSERT/DELETE) as landed.",
    cluster_by=["turbine_id"],
)
def bronze_turbine_cdc_copper_ibis():
    raw = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", RESCUED_COLUMN)
        .schema(TURBINE_CDC_SCHEMA)
        .load(TURBINES_PATH)
    )
    return add_ingest_metadata(raw)
